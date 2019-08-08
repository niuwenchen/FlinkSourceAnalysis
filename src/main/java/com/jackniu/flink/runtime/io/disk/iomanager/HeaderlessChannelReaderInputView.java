package com.jackniu.flink.runtime.io.disk.iomanager;

import com.jackniu.flink.core.memory.MemorySegment;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class HeaderlessChannelReaderInputView extends ChannelReaderInputView
{
    private int numBlocksRemaining;		// the number of blocks not yet consumed

    private final int lastBlockBytes;	// the number of valid bytes in the last block

    /**
     * Creates a new channel reader that reads from the given channel, expecting a specified
     * number of blocks in the channel, and returns only a specified number of bytes from
     * the last block.
     * <p>
     * WARNING: If the number of blocks given here is higher than the number of blocks in the
     * channel, then the last blocks will not be filled by the reader and will contain
     * undefined data.
     *
     * @param reader The reader that reads the data from disk back into memory.
     * @param memory A list of memory segments that the reader uses for reading the data in. If the
     *               list contains more than one segment, the reader will asynchronously pre-fetch
     *               blocks ahead.
     * @param numBlocks The number of blocks this channel will read.
     * @param numBytesInLastBlock The number of valid bytes in the last block.
     * @param waitForFirstBlock A flag indicating weather this constructor call should block
     *                          until the first block has returned from the asynchronous I/O reader.
     *
     * @throws IOException Thrown, if the read requests for the first blocks fail to be
     *                     served by the reader.
     */
    public HeaderlessChannelReaderInputView(BlockChannelReader<MemorySegment> reader, List<MemorySegment> memory, int numBlocks,
                                            int numBytesInLastBlock, boolean waitForFirstBlock)
            throws IOException
    {
        super(reader, memory, numBlocks, 0, waitForFirstBlock);

        this.numBlocksRemaining = numBlocks;
        this.lastBlockBytes = numBytesInLastBlock;
    }


    @Override
    protected MemorySegment nextSegment(MemorySegment current) throws IOException {
        // check for end-of-stream
        if (this.numBlocksRemaining <= 0) {
            this.reader.close();
            throw new EOFException();
        }

        // send a request first. if we have only a single segment, this same segment will be the one obtained in
        // the next lines
        if (current != null) {
            sendReadRequest(current);
        }

        // get the next segment
        this.numBlocksRemaining--;
        return this.reader.getNextReturnedBlock();
    }


    @Override
    protected int getLimitForSegment(MemorySegment segment) {
        return this.numBlocksRemaining > 0 ? segment.size() : this.lastBlockBytes;
    }
}