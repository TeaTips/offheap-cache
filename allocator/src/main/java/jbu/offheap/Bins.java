package jbu.offheap;

import jbu.exception.InvalidParameterException;

import static jbu.Primitive.*;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

abstract class Bins {

    /**
     * Use 64 bits for storing length for keeping a 64 align
     */
    static final int LENGTH_OFFSET = 8;

    protected static final int FREE = 0;
    protected static final int USED = 1;

    // Chunk real size
    final int realChunkSize;
    // Userdata chunk size
    final int userDataChunkSize;
    // Base addr of bin
    final int baseAddr;

    final int size;

    final AtomicInteger occupation = new AtomicInteger(0);

    /**
     * Status of memory chunk (allocated, free)
     */
    final AtomicIntegerArray chunks;

    // Helper for find free chunk
    final AtomicInteger chunkOffset = new AtomicInteger(0);

    protected Bins(int initialChunkNumber, int realChunkSize, int baseAddr) {
        this.size = initialChunkNumber;
        this.realChunkSize = realChunkSize;
        this.baseAddr = baseAddr;

        if (initialChunkNumber <= 0) {
            // Throw exception
            throw new InvalidParameterException("InitialChunkNumber must be > 0 ");
        }
        if (realChunkSize <= 0) {
            // Throw exception
            throw new InvalidParameterException("realChunkSize must be > 0 ");
        }

        // In a chunk we also store chunck size in int. Adding LENGTH_OFFSET byte
        // And addr of next chunk. Adding 8 byte
        // User data is realSize minus this
        this.userDataChunkSize = realChunkSize - LENGTH_OFFSET - LONG_LENGTH;
        this.chunks = new AtomicIntegerArray(initialChunkNumber);
    }

    long allocateOneChunk() {
        // Check if they are some chunk free
        if (occupation.get() < size) {
            int currentChunkOffset = this.chunkOffset.get();
            for (int i = 0; i < this.size; i++) {
                int currentChunkIndex = (i + currentChunkOffset) % this.size;
                if (chunks.compareAndSet(currentChunkIndex, FREE, USED)) {
                    this.chunkOffset.set(currentChunkOffset + i + 1);
                    occupation.incrementAndGet();
                    return AddrAlign.constructAddr(baseAddr, currentChunkIndex);
                }
            }
        }
        // Cannot allocate one chunk
        return -1;
    }

    long[] allocateNChunk(int n) {
        // Check parameter
        if (n <= 0) {
            return null;
        }
        long[] res = new long[n];
        int nbChunckAllocated = 0;
        // Search for n free chunk
        int currentChunkOffet = this.chunkOffset.get();
        for (int i = 0; i < this.size; i++) {
            int currentChunkIndex = (i + currentChunkOffet) % this.size;
            if (chunks.compareAndSet(currentChunkIndex, FREE, USED)) {
                res[nbChunckAllocated] = AddrAlign.constructAddr(baseAddr, currentChunkIndex);
            } else {
                continue;
            }
            if (++nbChunckAllocated == n) {
                this.chunkOffset.set(currentChunkIndex + 1);
                occupation.getAndAdd(nbChunckAllocated);
                return res;
            }
        }
        // Not enough chunk. Unallocate
        for (int i = 0; i < nbChunckAllocated; i++) {
            chunks.compareAndSet(AddrAlign.getChunkId(res[i]), USED, FREE);
        }
        occupation.getAndAdd(-nbChunckAllocated);
        return null;
    }

    /**
     * Free previously allocatedChunk
     *
     * @param chunks
     */
    void freeChunk(long... chunks) {
        for (long chunkAdr : chunks) {
            this.chunks.set(AddrAlign.getChunkId(chunkAdr), FREE);
            occupation.decrementAndGet();
        }
        this.chunkOffset.set(AddrAlign.getChunkId(chunks[0]));
    }

    abstract void setNextChunkId(int currentChunkId, long nextChunkId);

    abstract long getNextChunkId(int currentChunkId);

    abstract boolean storeInChunk(int chunkId, byte[] data, int currentOffset, int length);

    abstract boolean storeInChunk(int currentChunkId, ByteBuffer data);

    abstract byte[] loadFromChunk(int chunkId);

    long findOffsetForChunkId(int chunkId) {
        return (long) chunkId * (long) realChunkSize;
    }

    public int getAllocatedChunks() {
        return occupation.intValue();
    }

    public int getUsedSize() {
        return occupation.intValue() * realChunkSize;
    }
}
