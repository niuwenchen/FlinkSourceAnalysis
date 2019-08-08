package com.jackniu.flink.util;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface MemoryBlockIterator {
    /**
     * Move the iterator to the next memory block. The next memory block starts at the first element that was not
     * in the block before. A special case is when no record was in the block before, which happens when this
     * function is invoked two times directly in a sequence, without calling hasNext() or next in between. Then
     * the block moves one element.
     *
     * @return True if a new memory block was loaded, false if there were no further
     *         records and hence no further memory block.
     *
     * @throws IOException Thrown, when advancing to the next block failed.
     */
    public boolean nextBlock() throws IOException;
}
