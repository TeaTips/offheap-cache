package jbu.offheap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static jbu.UnsafeUtil.unsafe;
import static jbu.Primitive.*;

public class LoadContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(StoreContext.class);
    private static final Boolean LOGGER_IS_TRACE_ENABLED = LOGGER.isTraceEnabled();
    private static final Boolean LOGGER_IS_DEBUG_ENABLED = LOGGER.isDebugEnabled();

    private long firstChunkAdr;
    private long currentBaseAdr;
    private int currentOffset;
    private int remaining;
    private final Allocator allocator;

    LoadContext(Allocator allocator, long firstChunkAdr) {
        this.firstChunkAdr = firstChunkAdr;
        this.allocator = allocator;
        beginNewChunk(firstChunkAdr);
    }


    public void reset() {
        beginNewChunk(this.firstChunkAdr);
    }

    /* Current implementation of all loadPrimitive have bad performance. Use sparingly */

    public boolean loadBoolean() {
        boolean res = unsafe.getBoolean(null, this.currentBaseAdr + this.currentOffset);
        this.currentOffset += BOOLEAN_LENGTH;
        this.remaining -= BOOLEAN_LENGTH;
        return res;
    }

    public char loadChar() {
        if (LOGGER_IS_TRACE_ENABLED) {
            LOGGER.trace("load_char, remaining {}", remaining);
        }

        if (this.remaining >= CHAR_LENGTH) {
            // Int can be loaded in one time
            char res = unsafe.getChar(this.currentBaseAdr + this.currentOffset);
            this.currentOffset += CHAR_LENGTH;
            this.remaining -= CHAR_LENGTH;
            return res;
        } else {
            // Load all remaining bytes of the chunk (or remaining byte for primitive) in a buffer and begin load of a new chunk
            int primitiveSize = CHAR_LENGTH;
            int byteRemaining = CHAR_LENGTH;
            long bufAddr = unsafe.allocateMemory(CHAR_LENGTH);
            try {
                partialChunkLoad(primitiveSize, byteRemaining, bufAddr);
                char res = unsafe.getChar(bufAddr);
                LOGGER.trace("load_char_partial, final_value: {}, bin: {}", res, Integer.toBinaryString(res));
                return res;
            } finally {
                unsafe.freeMemory(bufAddr);
            }
        }
    }

    public byte loadByte() {
        byte res = unsafe.getByte(this.currentBaseAdr + this.currentOffset);
        this.currentOffset += BYTE_LENGTH;
        this.remaining -= BYTE_LENGTH;
        return res;
    }

    public short loadShort() {
        if (LOGGER_IS_TRACE_ENABLED) {
            LOGGER.trace("load_short, remaining {}", remaining);
        }

        if (this.remaining >= SHORT_LENGTH) {
            // Int can be loaded in one time
            short res = unsafe.getShort(this.currentBaseAdr + this.currentOffset);
            this.currentOffset += SHORT_LENGTH;
            this.remaining -= SHORT_LENGTH;
            return res;
        } else {
            // Load all remaining bytes of the chunk (or remaining byte for primitive) in a buffer and begin load of a new chunk
            int primitiveSize = SHORT_LENGTH;
            int byteRemaining = SHORT_LENGTH;
            long bufAddr = unsafe.allocateMemory(SHORT_LENGTH);
            try {
                partialChunkLoad(primitiveSize, byteRemaining, bufAddr);
                short res = unsafe.getShort(bufAddr);
                LOGGER.trace("load_short_partial, final_value: {}, bin: {}", res, Integer.toBinaryString(res));
                return res;
            } finally {
                unsafe.freeMemory(bufAddr);
            }
        }
    }

    public int loadInt() {
        if (LOGGER_IS_TRACE_ENABLED) {
            LOGGER.trace("load_int, remaining {}", remaining);
        }

        if (this.remaining >= INT_LENGTH) {
            // Int can be loaded in one time
            int res = unsafe.getInt(this.currentBaseAdr + this.currentOffset);
            this.currentOffset += INT_LENGTH;
            this.remaining -= INT_LENGTH;
            return res;
        } else {
            // Load all remaining bytes of the chunk (or remaining byte for primitive) in a buffer and begin load of a new chunk
            int primitiveSize = INT_LENGTH;
            int byteRemaining = INT_LENGTH;
            long bufAddr = unsafe.allocateMemory(INT_LENGTH);
            try {
                partialChunkLoad(primitiveSize, byteRemaining, bufAddr);
                int res = unsafe.getInt(bufAddr);
                LOGGER.trace("load_int_partial, final_value: {}, bin: {}", res, Integer.toBinaryString(res));
                return res;
            } finally {
                unsafe.freeMemory(bufAddr);
            }
        }
    }


    public float loadFloat() {
        if (LOGGER_IS_TRACE_ENABLED) {
            LOGGER.trace("load_float, remaining {}", remaining);
        }

        if (this.remaining >= FLOAT_LENGTH) {
            // Int can be loaded in one time
            float res = unsafe.getFloat(this.currentBaseAdr + this.currentOffset);
            this.currentOffset += FLOAT_LENGTH;
            this.remaining -= FLOAT_LENGTH;
            return res;
        } else {
            // Load all remaining bytes of the chunk (or remaining byte for primitive) in a buffer and begin load of a new chunk
            int primitiveSize = FLOAT_LENGTH;
            int byteRemaining = FLOAT_LENGTH;
            long bufAddr = unsafe.allocateMemory(FLOAT_LENGTH);
            try {
                partialChunkLoad(primitiveSize, byteRemaining, bufAddr);
                int res = unsafe.getInt(bufAddr);
                LOGGER.trace("load_float_partial, final_value: {}, bin: {}", res, Integer.toBinaryString(res));
                // When store chunked transform float to raw int bits
                return Float.intBitsToFloat(res);
            } finally {
                unsafe.freeMemory(bufAddr);
            }
        }
    }

    public double loadDouble() {
        if (LOGGER_IS_TRACE_ENABLED) {
            LOGGER.trace("load_float, remaining {}", remaining);
        }

        if (this.remaining >= DOUBLE_LENGTH) {
            // Int can be loaded in one time
            double res = unsafe.getDouble(this.currentBaseAdr + this.currentOffset);
            this.currentOffset += DOUBLE_LENGTH;
            this.remaining -= DOUBLE_LENGTH;
            return res;
        } else {
            // Load all remaining bytes of the chunk (or remaining byte for primitive) in a buffer and begin load of a new chunk
            int primitiveSize = DOUBLE_LENGTH;
            int byteRemaining = DOUBLE_LENGTH;
            long bufAddr = unsafe.allocateMemory(DOUBLE_LENGTH);
            try {
                partialChunkLoad(primitiveSize, byteRemaining, bufAddr);
                long res = unsafe.getLong(bufAddr);
                LOGGER.trace("load_double_partial, final_value: {}, bin: {}", res, Long.toBinaryString(res));
                // When store chunked transform float to raw int bits
                return Double.longBitsToDouble(res);
            } finally {
                unsafe.freeMemory(bufAddr);
            }
        }
    }

    public long loadLong() {
        if (LOGGER_IS_TRACE_ENABLED) {
            LOGGER.trace("load_long, remaining {}", remaining);
        }

        if (this.remaining >= LONG_LENGTH) {
            // Int can be loaded in one time
            long res = unsafe.getLong(this.currentBaseAdr + this.currentOffset);
            this.currentOffset += LONG_LENGTH;
            this.remaining -= LONG_LENGTH;
            return res;
        } else {
            // Load all remaining bytes of the chunk (or remaining byte for primitive) in a buffer and begin load of a new chunk
            int primitiveSize = LONG_LENGTH;
            int byteRemaining = LONG_LENGTH;
            long bufAddr = unsafe.allocateMemory(LONG_LENGTH);
            try {
                partialChunkLoad(primitiveSize, byteRemaining, bufAddr);
                long res = unsafe.getLong(bufAddr);
                LOGGER.trace("load_long_partial, final_value: {}, bin: {}", res, Long.toBinaryString(res));
                return res;
            } finally {
                unsafe.freeMemory(bufAddr);
            }
        }
    }

    private void partialChunkLoad(int primitiveSize, int byteRemaining, long bufAddr) {
        do {
            if (this.remaining >= BYTE_LENGTH) {
                // Load one byte
                byte b = unsafe.getByte(this.currentBaseAdr + this.currentOffset);
                // put them into int
                unsafe.putByte(null, bufAddr + primitiveSize - byteRemaining, b);
                byteRemaining -= BYTE_LENGTH;
                this.remaining -= BYTE_LENGTH;
                this.currentOffset += BYTE_LENGTH;
            } else if (this.remaining == 0) {
                // Load a new chunk
                if (LOGGER_IS_DEBUG_ENABLED) {
                    LOGGER.debug("chunk_empty, remaining: {}", byteRemaining);
                }
                // Get next chunk address in last 4 byte
                beginNewChunk(unsafe.getLong(this.currentBaseAdr + this.currentOffset));
            }
        } while (byteRemaining > 0);
    }


    // NOT USE IT for another thing than array .... NYI (Not yet implemented)
    // Can be used for array...
    public void loadArray(Object dest, final long offset, final int arrayLength) {
        if (LOGGER_IS_TRACE_ENABLED) {
            LOGGER.trace("load_array, to: {}, offset: {}, length: {}, remaining: {}", dest, offset, arrayLength, this.remaining);
        }
        int byteRemaining = arrayLength;
        do {
            int byteToCopy = (byteRemaining > remaining) ? remaining : byteRemaining;
            //int d = unsafe.getInt(this.currentBaseAdr + this.currentOffset);
            // FIXME WHY DIRECT MEMORY DON'T WORK !!!!!!!!!! (throw illegalArgument)
            // FIXME Marked as NotYetImplemented in code work only if destination are primitive array
            // See http://mail.openjdk.java.net/pipermail/hotspot-runtime-dev/2012-March/003322.html
            unsafe.copyMemory(null, this.currentBaseAdr + this.currentOffset, dest,
                    offset + (arrayLength - byteRemaining), byteToCopy);

            byteRemaining -= byteToCopy;

            // Update LoadContext state
            this.currentOffset += byteToCopy;
            this.remaining -= byteToCopy;

            if (this.remaining == 0) {
                if (LOGGER_IS_DEBUG_ENABLED) {
                    LOGGER.debug("chunk_empty, remaining: {}", byteRemaining);
                }
                // Get next chunk address in last 4 byte
                beginNewChunk(unsafe.getLong(this.currentBaseAdr + this.currentOffset));
            }
        } while (byteRemaining > 0);
    }

    // loadArray has a better impl, but working only when jdk7 are really fully implemented...
    // Workaround for copy memory
    public void loadPrimitive(Object dest, final long destOffset, final int primitiveLength) {
        if (LOGGER_IS_TRACE_ENABLED) {
            LOGGER.trace("load_primitive, dest: {}, offset: {}, length: {}, chunk_remaining: {}",
                    dest, destOffset, primitiveLength, this.remaining);
        }
        int totalByteRemaining = primitiveLength;
        do {
            int byteToCopy = (totalByteRemaining > remaining) ? remaining : totalByteRemaining;
            int byteRemaining = byteToCopy;

            // primitive take 8,4,2 or 1 byte
            // Take the greatest size under totalByteRemaining
            if (byteRemaining / LONG_LENGTH == 1) {
                // copy 8 byte
                long b = unsafe.getLong(null, this.currentBaseAdr + this.currentOffset);
                unsafe.putLong(dest, destOffset + (primitiveLength - byteRemaining), b);
                byteRemaining -= LONG_LENGTH;
            }
            if (byteRemaining / INT_LENGTH == 1) {
                // copy 4 byte
                int b = unsafe.getInt(null, this.currentBaseAdr + this.currentOffset);
                if (LOGGER_IS_TRACE_ENABLED) {
                    LOGGER.trace("load_partial_primitive, bits: {}", Integer.toBinaryString(b));
                }
                unsafe.putInt(dest, destOffset + (primitiveLength - byteRemaining), b);
                byteRemaining -= INT_LENGTH;
            }
            if (byteRemaining / SHORT_LENGTH == 1) {
                // copy 2 byte
                short b = unsafe.getShort(null, this.currentBaseAdr + this.currentOffset);
                unsafe.putShort(dest, destOffset + (primitiveLength - byteRemaining), b);
                byteRemaining -= SHORT_LENGTH;
            }
            if (byteRemaining / BYTE_LENGTH == 1) {
                // copy 1 byte
                byte b = unsafe.getByte(null, this.currentBaseAdr + this.currentOffset);
                unsafe.putByte(dest, destOffset + (primitiveLength - byteRemaining), b);
                byteRemaining -= BYTE_LENGTH;
            }
            totalByteRemaining -= byteToCopy;
            this.remaining -= byteToCopy;
            this.currentOffset += byteToCopy;
            // If all chunk loaded take a new one
            if (this.remaining == 0) {
                // Get next chunk address in last 4 byte
                beginNewChunk(unsafe.getLong(this.currentBaseAdr + this.currentOffset));
            }
        } while (totalByteRemaining > 0);
    }


    private void beginNewChunk(long chunkAdr) {
        // get bins
        // FIXME Suport only unsafebin
        // Get baseAdr of allocated memory
        // Get baseOffset of chunk
        // And store this in currentBaseAdr
        UnsafeBins b = (UnsafeBins) allocator.getBinFromAddr(chunkAdr);
        this.currentBaseAdr = b.binAddr + b.findOffsetForChunkId(AddrAlign.getChunkId(chunkAdr));
        // Put the offset to 4... Don't read chunk size. Always same value as chunk size
        this.currentOffset = Bins.LENGTH_OFFSET;
        this.remaining = b.userDataChunkSize;

    }

}