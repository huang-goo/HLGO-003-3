/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.BitSet;

public class BloomFilter implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(BloomFilter.class);

    private final BitSet bitSet;
    private final int bitSetSize;
    private final int numberOfHashFunctions;
    private final int expectedInsertions;
    private final double falsePositiveProbability;
    private int insertions;

    public BloomFilter(int expectedInsertions, double falsePositiveProbability) {
        this.expectedInsertions = expectedInsertions;
        this.falsePositiveProbability = falsePositiveProbability;
        this.bitSetSize = calculateOptimalBitSetSize(expectedInsertions, falsePositiveProbability);
        this.numberOfHashFunctions = calculateOptimalNumberOfHashFunctions(expectedInsertions, bitSetSize);
        this.bitSet = new BitSet(bitSetSize);
        this.insertions = 0;
        logger.info("BloomFilter initialized: bitSetSize={}, hashFunctions={}, expectedInsertions={}, fpp={}",
                bitSetSize, numberOfHashFunctions, expectedInsertions, falsePositiveProbability);
    }

    private static int calculateOptimalBitSetSize(int n, double p) {
        return (int) Math.ceil(-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    private static int calculateOptimalNumberOfHashFunctions(int n, int m) {
        return (int) Math.ceil((double) m / n * Math.log(2));
    }

    private int[] hash(String element) {
        int[] hashes = new int[numberOfHashFunctions];
        long hash64 = MurmurHash.murmurHash64(element);
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        for (int i = 0; i < numberOfHashFunctions; i++) {
            int combinedHash = hash1 + i * hash2;
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            hashes[i] = combinedHash % bitSetSize;
        }
        return hashes;
    }

    public synchronized boolean mightContain(String element) {
        if (element == null) {
            return false;
        }
        int[] hashes = hash(element);
        for (int hash : hashes) {
            if (!bitSet.get(hash)) {
                return false;
            }
        }
        return true;
    }

    public synchronized boolean put(String element) {
        if (element == null) {
            return false;
        }
        if (mightContain(element)) {
            return false;
        }
        int[] hashes = hash(element);
        for (int hash : hashes) {
            bitSet.set(hash);
        }
        insertions++;
        return true;
    }

    public synchronized boolean putIfAbsent(String element) {
        return put(element);
    }

    public synchronized int getInsertions() {
        return insertions;
    }

    public synchronized double getExpectedFalsePositiveProbability() {
        return Math.pow(1 - Math.exp(-numberOfHashFunctions * (double) insertions / bitSetSize),
                numberOfHashFunctions);
    }

    public synchronized boolean isNearCapacity() {
        return insertions >= expectedInsertions * 0.9;
    }

    public synchronized void clear() {
        bitSet.clear();
        insertions = 0;
        logger.info("BloomFilter cleared");
    }

    public int getBitSetSize() {
        return bitSetSize;
    }

    public int getNumberOfHashFunctions() {
        return numberOfHashFunctions;
    }

    public int getExpectedInsertions() {
        return expectedInsertions;
    }

    public double getFalsePositiveProbability() {
        return falsePositiveProbability;
    }

    private static class MurmurHash {
        public static long murmurHash64(String data) {
            byte[] bytes = data.getBytes();
            long h1 = 0x9368e53c2f6af274L;
            long h2 = 0x586dcd208f7cd3fdL;
            long c1 = 0x87c37b91114253d5L;
            long c2 = 0x4cf5ad432745937fL;

            int length = bytes.length;
            int blockCount = length / 16;

            for (int i = 0; i < blockCount; i++) {
                int index = i * 16;
                long k1 = getLittleEndianLong(bytes, index);
                long k2 = getLittleEndianLong(bytes, index + 8);

                k1 *= c1;
                k1 = Long.rotateLeft(k1, 31);
                k1 *= c2;
                h1 ^= k1;

                h1 = Long.rotateLeft(h1, 27);
                h1 += h2;
                h1 = h1 * 5 + 0x52dce729;

                k2 *= c2;
                k2 = Long.rotateLeft(k2, 33);
                k2 *= c1;
                h2 ^= k2;

                h2 = Long.rotateLeft(h2, 31);
                h2 += h1;
                h2 = h2 * 5 + 0x38495ab5;
            }

            long k1 = 0;
            long k2 = 0;

            int tailStart = blockCount * 16;
            int tailLength = length & 15;

            switch (tailLength) {
                case 15:
                    k2 ^= (long) bytes[tailStart + 14] << 48;
                case 14:
                    k2 ^= (long) bytes[tailStart + 13] << 40;
                case 13:
                    k2 ^= (long) bytes[tailStart + 12] << 32;
                case 12:
                    k2 ^= (long) bytes[tailStart + 11] << 24;
                case 11:
                    k2 ^= (long) bytes[tailStart + 10] << 16;
                case 10:
                    k2 ^= (long) bytes[tailStart + 9] << 8;
                case 9:
                    k2 ^= bytes[tailStart + 8];
                    k2 *= c2;
                    k2 = Long.rotateLeft(k2, 33);
                    k2 *= c1;
                    h2 ^= k2;
                case 8:
                    k1 ^= (long) bytes[tailStart + 7] << 56;
                case 7:
                    k1 ^= (long) bytes[tailStart + 6] << 48;
                case 6:
                    k1 ^= (long) bytes[tailStart + 5] << 40;
                case 5:
                    k1 ^= (long) bytes[tailStart + 4] << 32;
                case 4:
                    k1 ^= (long) bytes[tailStart + 3] << 24;
                case 3:
                    k1 ^= (long) bytes[tailStart + 2] << 16;
                case 2:
                    k1 ^= (long) bytes[tailStart + 1] << 8;
                case 1:
                    k1 ^= bytes[tailStart];
                    k1 *= c1;
                    k1 = Long.rotateLeft(k1, 31);
                    k1 *= c2;
                    h1 ^= k1;
            }

            h1 ^= length;
            h2 ^= length;

            h1 += h2;
            h2 += h1;

            h1 = fmix64(h1);
            h2 = fmix64(h2);

            h1 += h2;
            h2 += h1;

            return h1;
        }

        private static long getLittleEndianLong(byte[] bytes, int index) {
            return ((long) bytes[index] & 0xff)
                    | ((long) bytes[index + 1] & 0xff) << 8
                    | ((long) bytes[index + 2] & 0xff) << 16
                    | ((long) bytes[index + 3] & 0xff) << 24
                    | ((long) bytes[index + 4] & 0xff) << 32
                    | ((long) bytes[index + 5] & 0xff) << 40
                    | ((long) bytes[index + 6] & 0xff) << 48
                    | ((long) bytes[index + 7] & 0xff) << 56;
        }

        private static long fmix64(long k) {
            k ^= k >>> 33;
            k *= 0xff51afd7ed558ccdL;
            k ^= k >>> 33;
            k *= 0xc4ceb9fe1a85ec53L;
            k ^= k >>> 33;
            return k;
        }
    }
}
