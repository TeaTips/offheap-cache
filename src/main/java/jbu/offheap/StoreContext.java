package jbu.offheap;

import jbu.exception.OutOfOffheapMemoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static jbu.Primitive.*;
import static jbu.UnsafeUtil.unsafe;

public class StoreContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(StoreContext.class);
    private static final Boolean LOGGER_IS_TRACE_ENABLED = LOGGER.isTraceEnabled();
    private static final Boolean LOGGER_IS_DEBUG_ENABLED = LOGGER.isDebugEnabled();

    private long firstChunkAdr;
    private long currentChunkAdr;
    private long currentBaseAdr;
    private int currentOffset;
    private int remaining;
    private final Allocator allocator;

    StoreContext(Allocator allocator, long firstChunkAdr) {
        this.firstChunkAdr = firstChunkAdr;
        this.currentChunkAdr = firstChunkAdr;
        this.allocator = allocator;
        beginNewChunk(firstChunkAdr);
    }

    public void reuse() {
        beginNewChunk(this.firstChunkAdr);
    }

    public void storeInt(int value) {
        if (LOGGER_IS_TRACE_ENABLED) {
            LOGGER.trace("store_int, value {}, binary_value: {}, remaining {}",
                    value, Integer.toBinaryString(value), remaining);
        }
        int byteRemaining = INT_LENGTH;
        if (this.remaining >= INT_LENGTH) {
            // copy all
            unsafe.putInt(this.currentBaseAdr + this.currentOffset, value);
            this.currentOffset += INT_LENGTH;
            this.remaining -= INT_LENGTH;
            byteRemaining -= INT_LENGTH;
        } else {
            storePartialPrimitive(value, byteRemaining, INT_LENGTH);
        }
    }


    public void storeLong(long value) {
        if (LOGGER_IS_TRACE_ENABLED) {
            LOGGER.trace("store_long, value {}, binary_value: {}, remaining {}",
                    value, Long.toBinaryString(value), remaining);
        }
        int byteRemaining = LONG_LENGTH;
        if (this.remaining >= LONG_LENGTH) {
            // copy all
            unsafe.putLong(this.currentBaseAdr + this.currentOffset, value);
            this.currentOffset += LONG_LENGTH;
            this.remaining -= LONG_LENGTH;
            byteRemaining -= LONG_LENGTH;
        } else {
            storePartialPrimitive(value, byteRemaining, LONG_LENGTH);
        }
    }

    public void storeShort(short value) {
        if (LOGGER_IS_TRACE_ENABLED) {
            LOGGER.trace("store_short, value {}, binary_value: {}, remaining {}",
                    value, Integer.toBinaryString(value), remaining);
        }
        int byteRemaining = SHORT_LENGTH;
        if (this.remaining >= SHORT_LENGTH) {
            // copy all
            unsafe.putInt(this.currentBaseAdr + this.currentOffset, value);
            this.currentOffset += SHORT_LENGTH;
            this.remaining -= SHORT_LENGTH;
            byteRemaining -= SHORT_LENGTH;
        } else {
            storePartialPrimitive(value, byteRemaining, SHORT_LENGTH);
        }
    }

    public void storeChar(char value) {
        if (LOGGER_IS_TRACE_ENABLED) {
            LOGGER.trace("store_char, value {}, binary_value: {}, remaining {}",
                    value, Integer.toBinaryString(value), remaining);
        }
        int byteRemaining = CHAR_LENGTH;
        if (this.remaining >= CHAR_LENGTH) {
            // copy all
            unsafe.putInt(this.currentBaseAdr + this.currentOffset, value);
            this.currentOffset += CHAR_LENGTH;
            this.remaining -= CHAR_LENGTH;
            byteRemaining -= CHAR_LENGTH;
        } else {
            storePartialPrimitive(value, byteRemaining, CHAR_LENGTH);
        }
    }

    public void storeFloat(float value) {
        if (LOGGER_IS_TRACE_ENABLED) {
            LOGGER.trace("store_float, value {}, binary_value: {}, remaining {}",
                    value, Double.toHexString(value), remaining);
        }
        int byteRemaining = FLOAT_LENGTH;
        if (this.remaining >= FLOAT_LENGTH) {
            // copy all
            //unsafe.putInt(this.currentBaseAdr + this.currentOffset, value);
            this.currentOffset += FLOAT_LENGTH;
            this.remaining -= FLOAT_LENGTH;
            byteRemaining -= FLOAT_LENGTH;
        } else {
            storePartialPrimitive(Float.floatToRawIntBits(value), byteRemaining, FLOAT_LENGTH);
        }
    }


    public void storeDouble(double value) {
        if (LOGGER_IS_TRACE_ENABLED) {
            LOGGER.trace("store_long, value {}, binary_value: {}, remaining {}",
                    value, Double.toHexString(value), remaining);
        }
        int byteRemaining = LONG_LENGTH;
        if (this.remaining >= LONG_LENGTH) {
            // copy all
            //unsafe.putInt(this.currentBaseAdr + this.currentOffset, value);
            this.currentOffset += LONG_LENGTH;
            this.remaining -= LONG_LENGTH;
            byteRemaining -= LONG_LENGTH;
        } else {
            storePartialPrimitive(Double.doubleToRawLongBits(value), byteRemaining, LONG_LENGTH);
        }
    }

    private void storePartialPrimitive(long value, int byteRemaining, int primitiveLength) {
        do {
            if (this.remaining >= BYTE_LENGTH) {
                // copy first byte
                if (LOGGER_IS_TRACE_ENABLED) {
                    LOGGER.trace("store_primitive_partial, partial_value: {}, binary: {} ",
                            ((byte) value >> (primitiveLength - byteRemaining) * 8),
                            Integer.toBinaryString(((byte) value >> (primitiveLength - byteRemaining) * 8)));
                }
                unsafe.putByte(this.currentBaseAdr + this.currentOffset,
                        (byte) (value >> (primitiveLength - byteRemaining) * 8));
                this.currentOffset += BYTE_LENGTH;
                this.remaining -= BYTE_LENGTH;
                byteRemaining -= BYTE_LENGTH;
            } else if (this.remaining == 0) {
                // next or alloc
                if (LOGGER_IS_DEBUG_ENABLED) {
                    LOGGER.debug("chunk_full, remaining: {}", byteRemaining);
                }
                // If not enough memory reserved. We can take more chunk at runtime
                // Get next chunk address in last 8 byte
                long nextChunkAdr = unsafe.getLong(this.currentBaseAdr + this.currentOffset);
                if (nextChunkAdr >= 0) {
                    beginNewChunk(nextChunkAdr);
                } else {
                    // Try to take more memory. Ask for a new chunk of same size as current
                    allocateAndBeginNewChunk();
                }
            }
        } while (byteRemaining > 0);
    }


    public void storeSomething(Object object, final long offset, final int length) {
        if (LOGGER_IS_TRACE_ENABLED) {
            LOGGER.trace("store_field, from: {}, offset: {}, length: {}, remaining: {}", object, offset, length, this.remaining);
        }
        int byteRemaining = length;
        do {
            // If remaining in currentChunk == 0 load next chunk
            if (this.remaining == 0) {
                if (LOGGER_IS_DEBUG_ENABLED) {
                    LOGGER.debug("chunk_full, remaining: {}", byteRemaining);
                }
                // If not enough memory reserved. We can take more chunk at runtime
                // Get next chunk address in last 8 byte
                long nextChunkAdr = unsafe.getLong(this.currentBaseAdr + this.currentOffset);
                if (nextChunkAdr >= 0) {
                    beginNewChunk(nextChunkAdr);
                } else {
                    // Try to take more memory. Ask for a new chunk of same size as current
                    allocateAndBeginNewChunk();
                }
            }
            int byteToCopy = (this.remaining > byteRemaining) ? byteRemaining : this.remaining;
            unsafe.copyMemory(object, offset + (length - byteRemaining), null, this.currentBaseAdr + this.currentOffset, byteToCopy);
            byteRemaining -= byteToCopy;
            this.currentOffset += byteToCopy;
            this.remaining -= byteToCopy;

        } while (byteRemaining > 0);
    }

    private void allocateAndBeginNewChunk() {
        // FIXME Why when no more memory can be allocated
        if (allocator.extend(currentChunkAdr)) {
            long nextChunkAdr = unsafe.getLong(this.currentBaseAdr + this.currentOffset);
            beginNewChunk(nextChunkAdr);
        } else {
            // no more memory
            throw new OutOfOffheapMemoryException("No more memory can be allocated");
        }

    }

    private void beginNewChunk(long chunkAdr) {
        // get bins
        // FIXME Suport only unsafebin
        // Get baseAdr of allocated memory
        // Get baseOffset of chunk
        // And store this in currentBaseAdr
        UnsafeBins b = (UnsafeBins) allocator.getBinFromAddr(chunkAdr);
        this.currentChunkAdr = chunkAdr;
        this.currentBaseAdr = b.binAddr + b.findOffsetForChunkId(AddrAlign.getChunkId(chunkAdr));
        this.currentOffset = 0;
        this.remaining = b.userDataChunkSize;
        // put the size of data in 4 first byte
        // FIXME with store context always use full size
        unsafe.putInt(this.currentBaseAdr + this.currentOffset, this.remaining);
        this.currentOffset += Bins.LENGTH_OFFSET;
    }


}