package com.jackniu.flink.runtime.io.disk;

import com.jackniu.flink.core.memory.MemorySegment;
import com.jackniu.flink.core.memory.MemorySegmentSource;
import com.jackniu.flink.runtime.memory.AbstractPagedOutputView;
import com.jackniu.flink.util.MathUtils;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class SimpleCollectingOutputView  extends AbstractPagedOutputView {

    private final List<MemorySegment> fullSegments;

    private final MemorySegmentSource memorySource;

    private final int segmentSizeBits;

    private int segmentNum;


    public SimpleCollectingOutputView(List<MemorySegment> fullSegmentTarget,
                                      MemorySegmentSource memSource, int segmentSize)
    {
        super(memSource.nextSegment(), segmentSize, 0);
        this.segmentSizeBits = MathUtils.log2strict(segmentSize);
        this.fullSegments = fullSegmentTarget;
        this.memorySource = memSource;
        this.fullSegments.add(getCurrentSegment());
    }


    public void reset() {
        if (this.fullSegments.size() != 0) {
            throw new IllegalStateException("The target list still contains memory segments.");
        }

        clear();
        try {
            advance();
        } catch (IOException ioex) {
            throw new RuntimeException("Error getting first segment for record collector.", ioex);
        }
        this.segmentNum = 0;
    }

    @Override
    protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent) throws EOFException {
        final MemorySegment next = this.memorySource.nextSegment();
        if (next != null) {
            this.fullSegments.add(next);
            this.segmentNum++;
            return next;
        } else {
            throw new EOFException();
        }
    }

    public long getCurrentOffset() {
        return (((long) this.segmentNum) << this.segmentSizeBits) + getCurrentPositionInSegment();
    }
}
