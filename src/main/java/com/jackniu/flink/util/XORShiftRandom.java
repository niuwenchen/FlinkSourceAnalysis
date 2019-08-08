package com.jackniu.flink.util;

import java.util.Random;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class XORShiftRandom extends Random {

    private static final long serialVersionUID = -825722456120842841L;
    private long seed;

    public XORShiftRandom() {
        this(System.nanoTime());
    }

    public XORShiftRandom(long input) {
        super(input);
        this.seed = MathUtils.murmurHash((int) input) ^ MathUtils.murmurHash((int) (input >>> 32));
    }

    /**
     * All other methods like nextInt()/nextDouble()... depends on this, so we just need to overwrite
     * this.
     *
     * @param bits Random bits
     * @return The next pseudorandom value from this random number
     * generator's sequence
     */
    @Override
    public int next(int bits) {
        long nextSeed = seed ^ (seed << 21);
        nextSeed ^= (nextSeed >>> 35);
        nextSeed ^= (nextSeed << 4);
        seed = nextSeed;
        return (int) (nextSeed & ((1L << bits) - 1));
    }
}
