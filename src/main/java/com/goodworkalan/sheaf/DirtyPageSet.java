package com.goodworkalan.sheaf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * A collection of pages whose byte buffers contain writes that have not been
 * flushed to disk.
 * <p>
 * 
 * @author Alan Gutierrez
 */
public final class DirtyPageSet {
    /** A map of page positions to hard references to raw pages. */
    private final Map<Long, RawPage> rawPages;

    /** A map of page positions to hard references to page byte buffers. */
    private final Map<Long, ByteBuffer> byteBuffers;

    /**
     * Construct a dirty page set.
     */
    public DirtyPageSet() {
        this.rawPages = new HashMap<Long, RawPage>();
        this.byteBuffers = new HashMap<Long, ByteBuffer>();
    }

    /**
     * Return the count of dirty pages held by this dirty page set.
     * 
     * @return The size of the set.
     */
    public int size() {
        return rawPages.size();
    }

    /**
     * Add a raw page to the dirty page set.
     * <p>
     * The dirty page set will hold a hard reference to the raw page and the
     * byte buffer of the hard page.
     * <p>
     * Because the byte buffer is softly referenced by the raw page, this method
     * must be called before writing to the byte buffer, in order to ensure that
     * writes to the byte buffer are not collected.
     * 
     * @param rawPage
     *            The dirty raw page.
     */
    public void add(RawPage rawPage) {
        rawPages.put(rawPage.getPosition(), rawPage);
        byteBuffers.put(rawPage.getPosition(), rawPage.getByteBuffer());
    }

    /**
     * Flush the dirty page set writing the dirty raw pages to the sheafs from
     * which they came and releasing all hard references to raw pages and their
     * associated byte buffers.
     */
    public void flush() {
        for (RawPage rawPage: rawPages.values())
        {
            Sheaf sheaf = rawPage.getSheaf();
            synchronized (rawPage) {
                try {
                    rawPage.write(sheaf.getFileChannel(), sheaf.getOffset());
                } catch (IOException e) {
                    throw new SheafException(101, e);
                }
            }
        }
        rawPages.clear();
        byteBuffers.clear();
    }

    /**
     * Release all hard references to raw pages and their associated byte
     * buffers without writing the changes.
     */
    public void clear() {
        rawPages.clear();
        byteBuffers.clear();
    }
}