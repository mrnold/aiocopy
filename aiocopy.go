package main

import (
	"bufio"
	"bytes"
	"fmt"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	libnbd "libguestfs.org/libnbd"
)

/* Section: Destination file operations */

// VDDKDataSink provides a mockable interface for saving data from the source.
type VDDKDataSink interface {
	Pwrite(buf []byte, offset uint64) (int, error)
	Write(buf []byte) (int, error)
	ZeroRange(offset int64, length int64) error
	Close()
}

// VDDKFileSink writes the source disk data to a local file.
type VDDKFileSink struct {
	file    *os.File
	writer  *bufio.Writer
	isBlock bool
}

func createVddkDataSink(destinationFile string, size uint64) (VDDKDataSink, error) {
	isBlock := false

	flags := os.O_WRONLY
	if !isBlock {
		flags |= os.O_CREATE
	}

	file, err := os.OpenFile(destinationFile, flags, 0644)
	if err != nil {
		return nil, err
	}

	writer := bufio.NewWriter(file)
	sink := &VDDKFileSink{
		file:    file,
		writer:  writer,
		isBlock: isBlock,
	}
	return sink, err
}

// Pwrite writes the given byte buffer to the sink at the given offset
func (sink *VDDKFileSink) Pwrite(buffer []byte, offset uint64) (int, error) {
	written, err := syscall.Pwrite(int(sink.file.Fd()), buffer, int64(offset))
	blocksize := len(buffer)
	if written < blocksize {
		fmt.Printf("%s Wrote less than blocksize (%d): %d\n", time.Now().Format(time.StampNano), blocksize, written)
	}
	if err != nil {
		fmt.Printf("%s Buffer write error: %s\n", time.Now().Format(time.StampNano), err)
	}
	return written, err
}

// Write appends the given buffer to the sink
func (sink *VDDKFileSink) Write(buf []byte) (int, error) {
	written, err := sink.writer.Write(buf)
	if err != nil {
		return written, err
	}
	err = sink.writer.Flush()
	return written, err
}

// ZeroRange fills the destination range with zero bytes
func (sink *VDDKFileSink) ZeroRange(offset int64, length int64) error {
	punch := func(offset int64, length int64) error {
		fmt.Printf("%s Punching %d-byte hole at offset %d\n", time.Now().Format(time.StampNano), length, offset)
		flags := uint32(unix.FALLOC_FL_PUNCH_HOLE | unix.FALLOC_FL_KEEP_SIZE)
		return syscall.Fallocate(int(sink.file.Fd()), flags, offset, length)
	}

	var err error
	if sink.isBlock { // Try to punch a hole in block device destination
		err = punch(offset, length)
	} else {
		var info os.FileInfo
		info, err = sink.file.Stat()
		if err != nil {
			fmt.Printf("%s Unable to stat destination file: %v\n", time.Now().Format(time.StampNano), err)
		} else { // Filesystem
			if offset+length > info.Size() { // Truncate only if extending the file
				err = syscall.Ftruncate(int(sink.file.Fd()), offset+length)
			} else { // Otherwise, try to punch a hole in the file
				err = punch(offset, length)
			}
		}
	}

	if err != nil { // Fall back to regular pwrite
		fmt.Printf("%s Unable to zero range %d - %d on destination, falling back to pwrite: %v\n", time.Now().Format(time.StampNano), offset, offset+length, err)
		//fmt.Printf("%s Unable to zero range %d - %d on destination, avoiding write for test %v\n", time.Now().Format(time.StampNano), offset, offset+length, err)
		//return nil
		err = nil
		count := int64(0)
		const blocksize = 16 << 20
		buffer := bytes.Repeat([]byte{0}, blocksize)
		for count < length {
			remaining := length - count
			if remaining < blocksize {
				buffer = bytes.Repeat([]byte{0}, int(remaining))
			}
			written, err := sink.Pwrite(buffer, uint64(offset))
			if err != nil {
				fmt.Printf("%s Unable to write %d zeroes at offset %d: %v\n", time.Now().Format(time.StampNano), length, offset, err)
				break
			}
			count += int64(written)
		}
	}

	return err
}

// Close closes the file after a transfer is complete.
func (sink *VDDKFileSink) Close() {
	logOnError(sink.writer.Flush())
	logOnError(sink.file.Sync())
	logOnError(sink.file.Close())
}

func logOnError(err error) {
	fmt.Printf("%s %v\n", time.Now().Format(time.StampNano), err)
}

/* Section: remote source file operations (libnbd) */

// MaxBlockStatusLength limits the maximum block status request size to 2GB
const MaxBlockStatusLength = (2 << 30)

// MaxPreadLengthESX limits individual VDDK data block transfers to 23MB.
// Larger block sizes fail immediately.
const MaxPreadLengthESX = (23 << 20)

// MaxPreadLengthVC limits indidivual VDDK data block transfers to 2MB only when
// connecting to vCenter. With vCenter endpoints, multiple simultaneous importer
// pods with larger read sizes cause allocation failures on the server, and the
// imports start to fail:
//
//	"NfcFssrvrProcessErrorMsg: received NFC error 5 from server:
//	 Failed to allocate the requested 24117272 bytes"
const MaxPreadLengthVC = (2 << 20)

// MaxPreadLengthNBD is the biggest pread length allowed by libnbd.
const MaxPreadLengthNBD = (64 << 20)

// MaxPreadLength is the maxmimum read size to request from VMware. Default to
// the larger option, and reduce it in createVddkDataSource when connecting to
// vCenter endpoints.
var MaxPreadLength = MaxPreadLengthESX

// NbdOperations provides a mockable interface for the things needed from libnbd.
type NbdOperations interface {
	GetSize() (uint64, error)
	AioPread(libnbd.AioBuffer, uint64, *libnbd.AioPreadOptargs) (uint64, error)
	Pread([]byte, uint64, *libnbd.PreadOptargs) error
	Close() *libnbd.LibnbdError
	AioBlockStatus(uint64, uint64, libnbd.ExtentCallback, *libnbd.AioBlockStatusOptargs) (uint64, error)
	BlockStatus(uint64, uint64, libnbd.ExtentCallback, *libnbd.BlockStatusOptargs) error
	Poll(timeout int) (uint, error)
	AioCommandCompleted(uint64) (bool, error)
	AioInFlight() (uint, error)
}

// BlockStatusData holds zero/hole status for one block of data
type BlockStatusData struct {
	Offset int64
	Length int64
	Flags  uint32
}

type AioBlockStatusResult struct {
	blocks  *[]*BlockStatusData
	err     error
	ready   bool
	started time.Time
	taken   time.Duration
}

type AioReadResult struct {
	buffer libnbd.AioBuffer
	offset uint64
	length int64
	flags  uint32
	err    error
	ready  bool
}

// CreateBlockUpdateCallback creates a list of block status results and a
// BlockStatus callback that fills out that list when called by libnbd.
func CreateBlockUpdateCallback() (*[]*BlockStatusData, func(string, uint64, []uint32, *int) int) {
	blocks := []*BlockStatusData{}

	// Callback for libnbd.BlockStatus. Needs to modify blocks list above.
	updateBlocksCallback := func(metacontext string, nbdOffset uint64, extents []uint32, err *int) int {
		fmt.Printf("%s updating %d blocks %p: %v\n", time.Now().Format(time.StampNano), len(extents), &blocks, extents)
		if nbdOffset > math.MaxInt64 {
			fmt.Printf("%s Block status offset too big for conversion: 0x%x\n", time.Now().Format(time.StampNano), nbdOffset)
			return -2
		}
		offset := int64(nbdOffset)

		if *err != 0 {
			fmt.Printf("%s Block status callback error at offset %d: error code %d\n", time.Now().Format(time.StampNano), offset, *err)
			return *err
		}
		if metacontext != "base:allocation" {
			fmt.Printf("%s Offset %d not base:allocation, ignoring\n", time.Now().Format(time.StampNano), offset)
			return 0
		}
		if (len(extents) % 2) != 0 {
			fmt.Printf("%s Block status entry at offset %d has unexpected length %d!\n", time.Now().Format(time.StampNano), offset, len(extents))
			return -1
		}
		for i := 0; i < len(extents); i += 2 {
			length, flags := int64(extents[i]), extents[i+1]
			if len(blocks) > 0 {
				fmt.Printf("%s update blocks current length %d\n", time.Now().Format(time.StampNano), len(blocks))
				last := len(blocks) - 1
				lastBlock := blocks[last]
				lastFlags := lastBlock.Flags
				lastOffset := lastBlock.Offset + lastBlock.Length
				if lastFlags == flags && lastOffset == offset {
					fmt.Printf("%s merging with previous block offset %d length %d\n", time.Now().Format(time.StampNano), offset, length)
					// Merge with previous block
					blocks[last] = &BlockStatusData{
						Offset: lastBlock.Offset,
						Length: lastBlock.Length + length,
						Flags:  lastFlags,
					}
				} else {
					fmt.Printf("%s adding full block offset %d length %d\n", time.Now().Format(time.StampNano), offset, length)
					blocks = append(blocks, &BlockStatusData{Offset: offset, Length: length, Flags: flags})
				}
			} else {
				fmt.Printf("%s update empty blocks list offset %d length %d\n", time.Now().Format(time.StampNano), offset, length)
				blocks = append(blocks, &BlockStatusData{Offset: offset, Length: length, Flags: flags})
			}
			offset += length
		}
		return 0
	}

	return &blocks, updateBlocksCallback
}

func CreateBlockStatusArguments(blocks *[]*BlockStatusData) (*AioBlockStatusResult, *libnbd.AioBlockStatusOptargs) {
	fmt.Printf("%s blocks from create blockstatusarguments: 0x%p 0x%p 0x%p\n", time.Now().Format(time.StampNano), &blocks, blocks, *blocks)
	if blocks == nil {
		return nil, nil
	}
	result := AioBlockStatusResult{
		blocks:  blocks,
		ready:   false,
		err:     nil,
		started: time.Now(),
	}
	completionCallback := func(error *int) int {
		if *error != 0 {
			fmt.Printf("%s error in block status completion callback: %d\n\n", time.Now().Format(time.StampNano), *error)
			result.err = syscall.Errno(*error)
			return -1
		}
		result.ready = true
		result.taken = time.Since(result.started)
		fmt.Printf("%s successfully completed block status command %d blocks included (0x%p) time taken %s\n", time.Now().Format(time.StampNano), len(*result.blocks), &(result.blocks), result.taken)
		return 1
	}
	return &result, &libnbd.AioBlockStatusOptargs{
		Flags:                 libnbd.CMD_FLAG_REQ_ONE,
		FlagsSet:              true,
		CompletionCallbackSet: true,
		CompletionCallback:    completionCallback,
	}
}

func CreatePreadArguments(result *AioReadResult) *libnbd.AioPreadOptargs {
	if result == nil {
		return nil
	}
	completionCallback := func(error *int) int {
		if *error != 0 {
			fmt.Printf("%s error in pread callback: %d\n", time.Now().Format(time.StampNano), *error)
			result.err = syscall.Errno(*error)
			return -1
		}
		// NBD pread is all or nothing, so shouldn't need to split up results based on how much was actually read
		result.ready = true
		fmt.Printf("%s successfully completed pread at %d len %d, (0x%p)\n", time.Now().Format(time.StampNano), result.offset, result.length, &result.buffer)
		return 1
	}
	return &libnbd.AioPreadOptargs{
		CompletionCallbackSet: true,
		CompletionCallback:    completionCallback,
	}
}

// AioCopyRange runs block status and pread commands asynchronously and writes
// disk data out to the destination.
func AioCopyRange(handle NbdOperations, sink VDDKDataSink, rangeStart, rangeLength int64, updateProgress func(int)) error {
	rangeEnd := rangeStart + rangeLength

	// Block status tracking
	statusCommands := []uint64{}                                   // Queue of block status command cookies
	statusResults := map[uint64]*AioBlockStatusResult{}            // Map of block status command cookies to results
	statusLength := uint64(min(rangeLength, MaxBlockStatusLength)) // Current request length, only changes when it needs to be shrunk for final block
	statusOffset := uint64(rangeStart)                             // Current status offset, does not necessarily advance by statusLength every time!

	// Read request tracking
	readCommands := []uint64{}
	readResults := map[uint64]*AioReadResult{}
	readDepth := 4 // Maximum concurrent downloads

	for {
		fmt.Printf("%s start polling loop, status queue %d, read queue %d, status offset %d\n", time.Now().Format(time.StampNano), len(statusCommands), len(readCommands), statusOffset)

		// Block status
		if len(statusCommands) < 1 && statusOffset < uint64(rangeEnd) { // Make sure one block status command is always running. Only one at a time because the NBD server might not return the exact requested length, so the next block status starting offset depends on the result of this one
			blocks, updateBlocksCallback := CreateBlockUpdateCallback()
			fmt.Printf("%s updating blocks 0x%p: %v\n", time.Now().Format(time.StampNano), blocks, blocks)
			result, blockStatusArguments := CreateBlockStatusArguments(blocks)

			fmt.Printf("%s issuing AIO block status length %d offset %d\n", time.Now().Format(time.StampNano), statusLength, statusOffset)
			command, err := handle.AioBlockStatus(statusLength, statusOffset, updateBlocksCallback, blockStatusArguments)
			if err != nil {
				return err
			}
			statusCommands = append(statusCommands, command)
			statusResults[command] = result
		}

		// When a block status result is ready, advance the offset of the next block status command
		if len(statusCommands) > 0 {
			lastStatusCommand := statusCommands[len(statusCommands)-1]
			lastStatusResult := statusResults[lastStatusCommand]
			if lastStatusResult.ready && len(*lastStatusResult.blocks) > 0 {
				lastBlock := (*lastStatusResult.blocks)[len(*lastStatusResult.blocks)-1]
				statusOffset = uint64(lastBlock.Offset + lastBlock.Length)
				statusLength = uint64(min(uint64(rangeEnd)-statusOffset, MaxBlockStatusLength))
				fmt.Printf("%s AIO status ready, advance offset %d length %d\n", time.Now().Format(time.StampNano), statusOffset, statusLength)
			}
		}

		// Read block
		for len(readCommands) < readDepth { // Read queue is not full, check if there's a block status command to consume
			if len(statusCommands) < 1 { // No status commands to check
				break
			}
			statusCommand := statusCommands[0]           // Always take the first one to service them in order
			if statusResults[statusCommand].err != nil { // Block status failed! Bail out? Or TODO copy whole block instead?
				return statusResults[statusCommand].err
			}
			if statusResults[statusCommand].ready { // Block status command is ready! Issue some reads
				fmt.Printf("%s AIO block status command ready! Inspecting returned blocks %p: %v\n", time.Now().Format(time.StampNano), &(statusResults[statusCommand].blocks), statusResults[statusCommand].blocks)
				block := (*statusResults[statusCommand].blocks)[0]

				if (block.Flags & (libnbd.STATE_ZERO | libnbd.STATE_HOLE)) != 0 { // If the range is empty, add a fake result for the write out later
					fmt.Printf("%s spoofing read command for empty range\n", time.Now().Format(time.StampNano))
					result := AioReadResult{
						length: block.Length,
						offset: uint64(block.Offset),
						flags:  block.Flags, // Writer will check this later
						ready:  true,
					}

					// Borrow status cookie and use it in the read command list,
					// because it is unique per libnbd handle and there is no
					// AioPread to generate a new one here.
					readCommand := statusCommand

					readCommands = append(readCommands, readCommand)
					readResults[readCommand] = &result
					b := (*statusResults[statusCommand].blocks)[1:]
					statusResults[statusCommand].blocks = &b
				} else { // Range is not empty, issue a real read
					var readCommand uint64
					var err error

					// Need to break buffer down into maximum pread-sized chunks,
					// removing chunks so that it can continue with this buffer
					// on the next loop when the queue is too full to handle all of them
					var result AioReadResult
					readSize := min(uint(block.Length), uint(MaxPreadLength))
					result.buffer = libnbd.MakeAioBuffer(readSize)
					result.length = int64(readSize)
					result.offset = uint64(block.Offset)
					readArguments := CreatePreadArguments(&result)
					fmt.Printf("%s issuing AIO pread at offset %d length %d\n", time.Now().Format(time.StampNano), result.offset, result.length)
					readCommand, err = handle.AioPread(result.buffer, result.offset, readArguments)
					if err != nil {
						return err
					}
					readCommands = append(readCommands, readCommand)
					readResults[readCommand] = &result

					fmt.Printf("%s advancing block, offset %d len %d bump %d bytes\n", time.Now().Format(time.StampNano), block.Offset, block.Length, readSize)
					block.Offset = block.Offset + int64(readSize)
					block.Length = block.Length - int64(readSize)
					fmt.Printf("%s resized block: offset %d len %d\n", time.Now().Format(time.StampNano), block.Offset, block.Length)
					if block.Length < 1 {
						fmt.Printf("%s done with block, discarding\n", time.Now().Format(time.StampNano))
						b := (*statusResults[statusCommand].blocks)[1:]
						statusResults[statusCommand].blocks = &b
					}
				}
				if len(*statusResults[statusCommand].blocks) == 0 { // All blocks from this status request have been consumed, remove from queue
					fmt.Printf("%s block status consumed, removing\n", time.Now().Format(time.StampNano))
					statusCommands = statusCommands[1:]
					delete(statusResults, statusCommand)
				}
			} else {
				fmt.Printf("%s block status running, not yet ready\n", time.Now().Format(time.StampNano))
				break // There's a block status command, but it's not ready yet. Break to continue polling
			}
		}

		// Write out first ready block, then back to loop so AIO queues don't have to wait on the writes that much
		if len(readCommands) > 0 {
			readCommand := readCommands[0]
			fmt.Printf("%s read queue ready to go depth %d\n", time.Now().Format(time.StampNano), len(readCommands))
			fmt.Printf("%s got first read command: %v\n", time.Now().Format(time.StampNano), readCommand)
			result := readResults[readCommand]
			fmt.Printf("%s got block from read: 0x%p: %v\n", time.Now().Format(time.StampNano), &result, result)
			if result.err != nil { // Error reading? Bail out
				return result.err
			}
			if result.ready { // One read result is ready! Write it out
				fmt.Printf("%s read result ready! offset %d length %d\n", time.Now().Format(time.StampNano), result.offset, result.length)
				if (result.flags & (libnbd.STATE_ZERO | libnbd.STATE_HOLE)) != 0 {
					skip := ""
					if (result.flags & libnbd.STATE_HOLE) != 0 {
						skip = "hole"
					}
					if (result.flags & libnbd.STATE_ZERO) != 0 {
						if skip != "" {
							skip += "/"
						}
						skip += "zero block"
					}
					fmt.Printf("%s Found a %d-byte %s at offset %d, filling destination with zeroes.\n", time.Now().Format(time.StampNano), result.length, skip, result.offset)
					dataSize -= uint64(result.length)
					if err := sink.ZeroRange(int64(result.offset), result.length); err != nil {
						return err
					}
					updateProgress(int(result.length))

				} else {
					count := int64(0)
					for count < int64(result.buffer.Size) {
						buffer := result.buffer.Bytes()[count:]
						writeOffset := result.offset + uint64(count)
						written, err := sink.Pwrite(buffer, writeOffset)
						if err != nil {
							fmt.Printf("%s %v\n", time.Now().Format(time.StampNano), fmt.Errorf("failed to write data block at offset %d to local file: %v", writeOffset, err))
							return err
						}
						fmt.Printf("%s wrote out %d bytes to offset %d\n", time.Now().Format(time.StampNano), written, writeOffset)
						updateProgress(written)
						count += int64(written)
					}
					result.buffer.Free()
				}

				if result.offset+uint64(result.length) >= uint64(rangeEnd) { // Immediately quit after writing out the last expected byte
					fmt.Println("finished transfer?")
					return nil
				}
				readCommands = readCommands[1:] // Pop this read result off the queue
				delete(readResults, readCommand)
			}
		}

		// Poll for any events from block status or pread
		inflight, err := handle.AioInFlight()
		if err != nil {
			fmt.Printf("%s failed to check for in-flight AIO commands: %v", time.Now().Format(time.StampNano), err)
		}
		if inflight > 0 {
			polltime := time.Now()
			fmt.Printf("%s polling for status/read events, %d in flight\n", time.Now().Format(time.StampNano), inflight)
			result, err := handle.Poll(int((10 * time.Minute).Milliseconds())) // Does this really stop and wait? Not much waiting seems to be happening
			if err != nil {
				return err
			}
			fmt.Printf("%s poll waited %s to get an event\n", time.Now().Format(time.StampNano), time.Since(polltime))
			if result == 0 {
				return fmt.Errorf("timed out waiting for poll at status offset %d", statusOffset)
			}
		} else {
			fmt.Printf("%s no commands in-flight, do not poll\n", time.Now().Format(time.StampNano))
		}
	}
}

// AioCopyRangeNoStatus runs pread commands asynchronously and writes // disk data out to the destination.
func AioCopyRangeNoStatus(handle NbdOperations, sink VDDKDataSink, rangeStart, rangeLength int64, updateProgress func(int)) error {
	rangeEnd := rangeStart + rangeLength

	// Read request tracking
	readCommands := []uint64{}
	readResults := map[uint64]*AioReadResult{}
	readDepth := 4 // Maximum concurrent downloads

	readOffset := int64(0)

	for {
		fmt.Printf("%s start polling loop, read queue %d, read offset %d\n", time.Now().Format(time.StampNano), len(readCommands), readOffset)

		// Read block
		for len(readCommands) < readDepth { // Read queue is not full, check if there's a block status command to consume
			var readCommand uint64
			var err error

			// Need to break buffer down into maximum pread-sized chunks,
			// removing chunks so that it can continue with this buffer
			// on the next loop when the queue is too full to handle all of them
			var result AioReadResult
			readSize := min(uint(rangeLength-readOffset), uint(MaxPreadLength))
			result.buffer = libnbd.MakeAioBuffer(readSize)
			result.length = int64(readSize)
			result.offset = uint64(readOffset)
			readArguments := CreatePreadArguments(&result)
			fmt.Printf("%s issuing AIO pread at offset %d length %d\n", time.Now().Format(time.StampNano), result.offset, result.length)
			readCommand, err = handle.AioPread(result.buffer, result.offset, readArguments)
			if err != nil {
				return err
			}
			readCommands = append(readCommands, readCommand)
			readResults[readCommand] = &result
			readOffset += int64(readSize)
		}

		// Write out first ready block, then back to loop so AIO queues don't have to wait on the writes that much
		for _, readCommand := range readCommands {
			fmt.Printf("%s read queue ready to go depth %d\n", time.Now().Format(time.StampNano), len(readCommands))
			fmt.Printf("%s got first read command: %v\n", time.Now().Format(time.StampNano), readCommand)
			result := readResults[readCommand]
			fmt.Printf("%s got block from read: 0x%p: %v\n", time.Now().Format(time.StampNano), &result, result)
			if result.err != nil { // Error reading? Bail out
				return result.err
			}
			if result.ready { // One read result is ready! Write it out
				fmt.Printf("%s read result ready! offset %d length %d\n", time.Now().Format(time.StampNano), result.offset, result.length)
				if (result.flags & (libnbd.STATE_ZERO | libnbd.STATE_HOLE)) != 0 {
					skip := ""
					if (result.flags & libnbd.STATE_HOLE) != 0 {
						skip = "hole"
					}
					if (result.flags & libnbd.STATE_ZERO) != 0 {
						if skip != "" {
							skip += "/"
						}
						skip += "zero block"
					}
					fmt.Printf("%s Found a %d-byte %s at offset %d, filling destination with zeroes.\n", time.Now().Format(time.StampNano), result.length, skip, result.offset)
					dataSize -= uint64(result.length)
					if err := sink.ZeroRange(int64(result.offset), result.length); err != nil {
						return err
					}
					updateProgress(int(result.length))

					if result.offset+uint64(result.length) >= uint64(rangeEnd) { // Immediately quit after writing out the last expected byte
						fmt.Println("finished transfer?")
						return nil
					}

					readCommands = readCommands[1:] // Pop this read result off the queue
					delete(readResults, readCommand)

					continue // Avoid polling after servicing a virtual command
				} else {
					count := int64(0)
					for count < int64(result.buffer.Size) {
						buffer := result.buffer.Bytes()[count:]
						writeOffset := result.offset + uint64(count)
						written, err := sink.Pwrite(buffer, writeOffset)
						if err != nil {
							fmt.Printf("%s %v\n", time.Now().Format(time.StampNano), fmt.Errorf("failed to write data block at offset %d to local file: %v", writeOffset, err))
							return err
						}
						fmt.Printf("%s wrote out %d bytes to offset %d\n", time.Now().Format(time.StampNano), written, writeOffset)
						updateProgress(written)
						count += int64(written)
					}
					result.buffer.Free()
					if result.offset+uint64(result.length) >= uint64(rangeEnd) { // Immediately quit after writing out the last expected byte
						fmt.Println("finished transfer?")
						return nil
					}

					readCommands = readCommands[1:] // Pop this read result off the queue
					delete(readResults, readCommand)
				}
			} else { // The next result isn't ready, break and find more work to do
				break
			}
		}

		// Poll for any events from block status or pread
		inflight, err := handle.AioInFlight()
		if err != nil {
			fmt.Printf("%s failed to check for in-flight AIO commands: %v", time.Now().Format(time.StampNano), err)
		}
		if inflight > 0 {
			polltime := time.Now()
			fmt.Printf("%s polling for status/read events, %d in flight\n", time.Now().Format(time.StampNano), inflight)
			result, err := handle.Poll(int((10 * time.Minute).Milliseconds())) // Does this really stop and wait? Not much waiting seems to be happening
			if err != nil {
				return err
			}
			fmt.Printf("%s poll waited %s to get an event\n", time.Now().Format(time.StampNano), time.Since(polltime))
			if result == 0 {
				return fmt.Errorf("timed out waiting for poll at read offset %d", readOffset)
			}
		} else {
			fmt.Printf("%s no commands in-flight, do not poll\n", time.Now().Format(time.StampNano))
		}
	}
}

type DiskChangeExtent struct {
	Length int64
	Start  int64
}

func SyncCopy(handle NbdOperations, sink VDDKDataSink, rangeStart, rangeLength int64, updateProgress func(int)) error {
	start := uint64(0)
	blocksize := uint64(MaxBlockStatusLength)
	for i := start; i < diskSize; i += blocksize {
		if (diskSize - i) < blocksize {
			blocksize = diskSize - i
		}

		extent := DiskChangeExtent{
			Length: int64(blocksize),
			Start:  int64(i),
		}

		blocks := GetBlockStatus(handle, extent)
		for _, block := range blocks {
			err := CopyRange(handle, sink, block, updateProgress)
			if err != nil {
				fmt.Printf("%s Unable to copy block at offset %d: %v\n", time.Now().Format(time.StampNano), block.Offset, err)
				return err
			}
		}
	}
	return nil
}

// Request blocks one at a time from libnbd
var fixedOptArgs = libnbd.BlockStatusOptargs{
	Flags:    libnbd.CMD_FLAG_REQ_ONE,
	FlagsSet: true,
}

// GetBlockStatus runs libnbd.BlockStatus on a given disk range.
// Translated from IMS v2v-conversion-host.
func GetBlockStatus(handle NbdOperations, extent DiskChangeExtent) []*BlockStatusData {
	var blocks []*BlockStatusData

	// Callback for libnbd.BlockStatus. Needs to modify blocks list above.
	updateBlocksCallback := func(metacontext string, nbdOffset uint64, extents []uint32, err *int) int {
		if nbdOffset > math.MaxInt64 {
			fmt.Printf("%s Block status offset too big for conversion: 0x%x\n", time.Now().Format(time.StampNano), nbdOffset)
			return -2
		}
		offset := int64(nbdOffset)

		if *err != 0 {
			fmt.Printf("%s Block status callback error at offset %d: error code %d\n", time.Now().Format(time.StampNano), offset, *err)
			return *err
		}
		if metacontext != "base:allocation" {
			fmt.Printf("%s Offset %d not base:allocation, ignoring\n", time.Now().Format(time.StampNano), offset)
			return 0
		}
		if (len(extents) % 2) != 0 {
			fmt.Printf("%s Block status entry at offset %d has unexpected length %d!\n", time.Now().Format(time.StampNano), offset, len(extents))
			return -1
		}
		for i := 0; i < len(extents); i += 2 {
			length, flags := int64(extents[i]), extents[i+1]
			if blocks != nil {
				last := len(blocks) - 1
				lastBlock := blocks[last]
				lastFlags := lastBlock.Flags
				lastOffset := lastBlock.Offset + lastBlock.Length
				if lastFlags == flags && lastOffset == offset {
					// Merge with previous block
					blocks[last] = &BlockStatusData{
						Offset: lastBlock.Offset,
						Length: lastBlock.Length + length,
						Flags:  lastFlags,
					}
				} else {
					blocks = append(blocks, &BlockStatusData{Offset: offset, Length: length, Flags: flags})
				}
			} else {
				blocks = append(blocks, &BlockStatusData{Offset: offset, Length: length, Flags: flags})
			}
			offset += length
		}
		return 0
	}

	if extent.Length < 1024*1024 {
		blocks = append(blocks, &BlockStatusData{
			Offset: extent.Start,
			Length: extent.Length,
			Flags:  0})
		return blocks
	}

	lastOffset := extent.Start
	endOffset := extent.Start + extent.Length
	for lastOffset < endOffset {
		var length int64
		missingLength := endOffset - lastOffset
		if missingLength > (MaxBlockStatusLength) {
			length = MaxBlockStatusLength
		} else {
			length = missingLength
		}
		createWholeBlock := func() []*BlockStatusData {
			block := &BlockStatusData{
				Offset: extent.Start,
				Length: extent.Length,
				Flags:  0,
			}
			blocks = []*BlockStatusData{block}
			return blocks
		}
		err := handle.BlockStatus(uint64(length), uint64(lastOffset), updateBlocksCallback, &fixedOptArgs)
		if err != nil {
			fmt.Printf("%s Error getting block status at offset %d, returning whole block instead. Error was: %v\n", time.Now().Format(time.StampNano), lastOffset, err)
			return createWholeBlock()
		}
		last := len(blocks) - 1
		newOffset := blocks[last].Offset + blocks[last].Length
		if lastOffset == newOffset {
			fmt.Printf("%s No new block status data at offset %d, returning whole block.\n", time.Now().Format(time.StampNano), newOffset)
			return createWholeBlock()
		}
		lastOffset = newOffset
	}

	return blocks
}

// CopyRange takes one data block, checks if it is a hole or filled with zeroes, and copies it to the sink
func CopyRange(handle NbdOperations, sink VDDKDataSink, block *BlockStatusData, updateProgress func(int)) error {
	skip := ""
	if (block.Flags & libnbd.STATE_HOLE) != 0 {
		skip = "hole"
	}
	if (block.Flags & libnbd.STATE_ZERO) != 0 {
		if skip != "" {
			skip += "/"
		}
		skip += "zero block"
	}

	if (block.Flags & (libnbd.STATE_ZERO | libnbd.STATE_HOLE)) != 0 {
		fmt.Printf("%s Found a %d-byte %s at offset %d, filling destination with zeroes.\n", time.Now().Format(time.StampNano), block.Length, skip, block.Offset)
		err := sink.ZeroRange(block.Offset, block.Length)
		dataSize -= uint64(block.Length)
		updateProgress(int(block.Length))
		return err
	}

	buffer := bytes.Repeat([]byte{0}, MaxPreadLength)
	count := int64(0)
	for count < block.Length {
		if block.Length-count < int64(MaxPreadLength) {
			buffer = bytes.Repeat([]byte{0}, int(block.Length-count))
		}
		length := len(buffer)

		offset := block.Offset + count
		err := handle.Pread(buffer, uint64(offset), nil)
		if err != nil {
			fmt.Printf("%s Error reading from source at offset %d: %v\n", time.Now().Format(time.StampNano), offset, err)
			return err
		}

		written, err := sink.Pwrite(buffer, uint64(offset))
		if err != nil {
			fmt.Printf("%s Failed to write data block at offset %d to local file: %v\n", time.Now().Format(time.StampNano), block.Offset, err)
			return err
		}

		updateProgress(written)
		count += int64(length)
	}
	return nil
}

func DumbCopy(handle NbdOperations, sink VDDKDataSink, rangeStart, rangeLength int64, updateProgress func(int)) error {
	buffer := bytes.Repeat([]byte{0}, MaxPreadLength)
	count := int64(0)
	for count < rangeLength {
		if rangeLength-count < int64(MaxPreadLength) {
			buffer = bytes.Repeat([]byte{0}, int(rangeLength-count))
		}
		fmt.Printf("%s Reading %d bytes at offset %d\n", time.Now().Format(time.StampNano), len(buffer), count)

		offset := rangeStart + count
		err := handle.Pread(buffer, uint64(offset), nil)
		if err != nil {
			fmt.Printf("%s Error dumb reading from source at offset %d: %v\n", time.Now().Format(time.StampNano), offset, err)
			return err
		}

		fmt.Printf("%s Writing %d bytes to offset %d\n", time.Now().Format(time.StampNano), len(buffer), offset)
		written, err := sink.Pwrite(buffer, uint64(offset))
		if err != nil {
			fmt.Printf("%s Failed to dumb write data block at offset %d to local file: %v\n", time.Now().Format(time.StampNano), rangeStart, err)
			return err
		}

		updateProgress(written)
		count += int64(written)
	}
	return nil
}

var diskSize uint64
var dataSize uint64
var currentProgressBytes uint64
var previousProgressPercent uint
var previousProgressBytes uint64
var initialProgressTime time.Time
var previousProgressTime time.Time

func updateProgress(written int) {
	// Only log progress at approximately 1% minimum intervals.
	currentProgressBytes += uint64(written)
	currentProgressPercent := uint(100.0 * (float64(currentProgressBytes) / float64(diskSize)))
	if currentProgressPercent > previousProgressPercent {
		progressMessage := fmt.Sprintf("%s Transferred %d/%d bytes (%d%%)", time.Now().Format(time.StampNano), currentProgressBytes, diskSize, currentProgressPercent)

		currentProgressTime := time.Now()
		overallProgressTime := uint64(time.Since(initialProgressTime).Seconds())
		if overallProgressTime > 0 {
			overallProgressRate := currentProgressBytes / overallProgressTime
			progressMessage += fmt.Sprintf(" at %d bytes/second overall", overallProgressRate)
		}

		progressTimeDifference := uint64(currentProgressTime.Sub(previousProgressTime).Seconds())
		if progressTimeDifference > 0 {
			progressSize := currentProgressBytes - previousProgressBytes
			progressRate := progressSize / progressTimeDifference
			progressMessage += fmt.Sprintf(", last 1%% was %d bytes at %d bytes/second", progressSize, progressRate)
		}

		fmt.Println(progressMessage)

		previousProgressBytes = currentProgressBytes
		previousProgressTime = currentProgressTime
		previousProgressPercent = currentProgressPercent
	}
}

func main() {
	go func() {
		http.ListenAndServe("0.0.0.0:10606", nil)
	}()
	handle, err := libnbd.Create()
	if err != nil {
		fmt.Printf("%s Unable to create libnbd handle: %v\n", time.Now().Format(time.StampNano), err)
		return
	}
	defer handle.Close()

	handle.AioInFlight()

	err = handle.AddMetaContext("base:allocation")
	if err != nil {
		fmt.Printf("%s Error adding base:allocation context to libnbd handle: %v\n", time.Now().Format(time.StampNano), err)
	}

	//socket, _ := url.Parse("nbd://" + nbdUnixSocket)
	//err = handle.ConnectUri("nbd+unix://?socket=" + nbdUnixSocket)
	err = handle.ConnectUri("nbd://localhost:10809")
	if err != nil {
		fmt.Printf("%s Unable to connect to socket: %v\n", time.Now().Format(time.StampNano), err)
		return
	}

	//diskSize = 25769803776
	diskSize, err = handle.GetSize()
	if err != nil {
		fmt.Printf("Unable to get NBD size: %v\n", err)
		return
	}
	dataSize = diskSize
	fmt.Printf("%s Disk size from NBD: %d\n", time.Now().Format(time.StampNano), diskSize)

	sink, err := createVddkDataSink("fedora.raw", diskSize)
	if err != nil {
		fmt.Printf("%s Failed to create fedora.raw: %v\n\n", time.Now().Format(time.StampNano), err)
		return
	}

	initialProgressTime = time.Now()
	fmt.Printf("%s is the starting timestamp\n", initialProgressTime.Format(time.StampNano))

	err = AioCopyRange(handle, sink, 0, int64(diskSize), updateProgress)
	if err != nil {
		fmt.Printf("%s Failed to Aio copy range: %v\n", time.Now(), err)
	}

	// err = AioCopyRangeNoStatus(handle, sink, 0, int64(diskSize), updateProgress)
	// if err != nil {
	// 	fmt.Printf("%s Failed to Aio copy range: %v\n", time.Now(), err)
	// }

	// err = SyncCopy(handle, sink, 0, int64(diskSize), updateProgress)
	// if err != nil {
	// 	fmt.Printf("%s Failed to copy synchronously: %v\n", time.Now().Format(time.StampNano), err)
	// }

	// err = DumbCopy(handle, sink, 0, int64(diskSize), updateProgress)
	// if err != nil {
	// 	fmt.Printf("%s Failed to copy dumbly: %v\n", time.Now().Format(time.StampNano), err)
	// 	return
	// }

	doneTime := time.Now()
	elapsed := doneTime.Sub(initialProgressTime).Seconds()
	fmt.Printf("%s is the final timestamp, calculating MD5\n", doneTime.Format(time.StampNano))
	fmt.Printf("Time diff: %f\n", elapsed)
	fmt.Printf("Rate w/zero: %f\n", ((float64(diskSize)/1024.0)/1024.0)/elapsed)
	fmt.Printf("Rate data only: %f\n", ((float64(dataSize)/1024.0)/1024.0)/elapsed)
	sum, err := exec.Command("md5sum", "fedora.raw").Output()
	if err != nil {
		fmt.Printf("failed to get MD5: %v\n", err)
	}
	fmt.Printf("MD5 sum: %s\n", sum)
}
