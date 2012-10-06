package jbu.offheap;

import static jbu.Primitive.*;
import static jbu.UnsafeUtil.unsafe;

import jbu.exception.BufferOverflowException;
import jbu.exception.InvalidJvmException;
import jbu.UnsafeReflection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

class UnsafeBins extends Bins implements UnsafeBinsMBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(UnsafeBins.class);

    static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;

    private long arrayBaseOffset = (long) unsafe.arrayBaseOffset(byte[].class);

    // Method for get address of a direct byte buffer
    private static final Field BUFFER_ADDR;

    static {
        try {
            BUFFER_ADDR = Buffer.class.getDeclaredField("address");
            BUFFER_ADDR.setAccessible(true);
        } catch (NoSuchFieldException e) {
            // Never append ??
            throw new InvalidJvmException("Cannot use UnsafeBins in this JVM", e);
        }
    }

    final long binAddr;

    UnsafeBins(int initialChunkNumber, int chunkSize, int baseAddr) {
        super(initialChunkNumber, chunkSize, baseAddr);
        long bufferSize = (long) initialChunkNumber * (long) realChunkSize;
        // FIXME Cannot allocate more than Integer.MAX_VALUE. Check this
        binAddr = unsafe.allocateMemory((long) initialChunkNumber * (long) realChunkSize);
        LOGGER.info("allocate_buffer, size: {} MB, chunk_size: {}, base_logical_addr: {}, begin_addr: {}"
                , bufferSize / 1024 / 1024, chunkSize, baseAddr, binAddr);
    }

    @Override
    boolean storeInChunk(int chunkId, byte[] data, final int currentOffset, final int length) {
        // check size for avoid overflow on other chunk
        if (length > this.userDataChunkSize) {
            throw new BufferOverflowException("Try to store too many data. Store "
                    + data.length + " in " + this.userDataChunkSize);
        }
        long baseAddr = findOffsetForChunkId(chunkId) + binAddr;

        // put the length
        unsafe.putInt(baseAddr, length);
        long dstAddr = baseAddr + LENGTH_OFFSET;
        long offset = arrayBaseOffset;
        int byteToStore = length;
        while (byteToStore > 0) {
            long size = (byteToStore > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : byteToStore;
            unsafe.copyMemory(data, offset + currentOffset, null, dstAddr, size);
            byteToStore -= size;
            offset += size;
            dstAddr += size;
        }
        return true;
    }

    @Override
    boolean storeInChunk(int chunkId, ByteBuffer data) {
        // Read only data between position and chunksize or buffer limit
        if (data.isDirect()) {
            // get base adress of the buffer
            int length = (data.remaining() > this.userDataChunkSize) ? this.userDataChunkSize : data.remaining();
            long dataAddr = UnsafeReflection.getLong(UnsafeBins.BUFFER_ADDR, data);
            long baseAddr = findOffsetForChunkId(chunkId) + binAddr;

            // put the length
            unsafe.putInt(baseAddr, length);
            // FIXME Who is this 4 ?
            long dstAddr = baseAddr + LENGTH_OFFSET;
            // put data from source position
            unsafe.copyMemory(dataAddr + (data.position() << 0), dstAddr, length);
            // update source position
            data.position(data.position() + length);
            return true;
        } else {
            // Easy way put part to save in a byte array and call standard storeInChunk
            byte[] tmp = new byte[this.userDataChunkSize];
            data.get(tmp);
            return storeInChunk(chunkId, tmp, 0, this.userDataChunkSize);
        }
    }

    @Override
    byte[] loadFromChunk(int chunkId) {
        long baseAddr = binAddr + findOffsetForChunkId(chunkId);
        int size = unsafe.getInt(baseAddr);
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = unsafe.getByte(baseAddr + LENGTH_OFFSET + i);
        }
        return data;
    }

    @Override
    void setNextChunkId(int currentChunkId, long nextChunkId) {
        // Set nextChunkId to last 8 bytes of currentChunkId
        long nextChunkOffset = binAddr + findOffsetForChunkId(currentChunkId) + userDataChunkSize + LENGTH_OFFSET;
        //System.out.println(nextChunkOffset + "  " + nextChunkId + " " + findOffsetForChunkId(currentChunkId));
        unsafe.putLong(nextChunkOffset, nextChunkId);
    }

    @Override
    long getNextChunkId(int currentChunkId) {
        // Set nextChunkId to last 8 bytes of currentChunkId
        long nextChunkOffset = binAddr + findOffsetForChunkId(currentChunkId) + userDataChunkSize + LENGTH_OFFSET;
        return unsafe.getLong(nextChunkOffset);
    }

}
