package com.goodworkalan.sheaf;

import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.goodworkalan.region.DirtyByteMap;
import com.goodworkalan.region.Writable;

/**
 * A representation of the basic page attributes of position and byte buffer
 * associated with an application of the raw page implemented by {@link Page}.
 * The raw page is always the page size of the Pack file and is always aligned
 * to a page boundary.
 * <p>
 * The raw page is participant in a strategy design pattern, where the content
 * of the bytes at the position are interpreted by an associated implementation
 * of {@link Page}.
 * <p>
 * The raw page maintains soft reference to a byte buffer of the bytes of the
 * raw page and is itself soft referenced within the map of pages by position
 * within {@link Sheaf}.
 * <p>
 * The soft references allow both the raw page and the byte buffer to be
 * reclaimed and reclaimed separately.
 * <p>
 * All pages write their changes out to the byte buffer. We hold onto a hard
 * referenced to dirty raw pages and we hold onto a hard reference to their
 * associated byte buffers in a dirty page map. The dirty page map will write
 * the dirty pages to file. Once the raw page is written to disk we let go of
 * the hard reference to the raw page and byte buffer in the raw page map. It
 * can be collected. The separate soft references only benefit the data page
 * which maintains a concurrency lock in the data page object.
 */
public final class RawPage implements Writable {
    /** The sheaf that manages this raw page. */
    private final Sheaf sheaf;

    private final Lock lock;

    private final DirtyByteMap dirtyByteMap;

    private long position;

    /**
     * A reference to a class that implements a specific application of a raw
     * page, the strategy for this page according to the strategy design
     * pattern.
     */
    private Page page;

    /**
     * A reclaimable reference to a byte buffer of the contents of the raw page.
     * The byte buffer is lazy loaded when needed and can be reclaimed whenever
     * there is no hard reference to the byte buffer. We keep a hard reference
     * to the byte buffer using a dirty page map, which gathers dirty buffers
     * and writes them out in a batch.
     */
    private Reference<ByteBuffer> byteBufferReference;

    /**
     * Get the page size boundary aligned position of the page in file.
     * 
     * @return The position of the page.
     */
    public synchronized long getPosition() {
        return position;
    }

    /**
     * Set the page size boundary aligned position of the page in file.
     * 
     * @param position
     *            The position of the page.
     */
    protected synchronized void setPosition(long position) {
        this.position = position;
    }

    /**
     * Create a raw page at the specified position associated with the specified
     * sheaf. The byte buffer is not loaded until the {@link #getByteBuffer
     * getByteBuffer} method is called.
     * 
     * @param sheaf
     *            The page manager.
     * @param position
     *            The position of the page.
     */
    public RawPage(Sheaf sheaf, long position) {
        this.dirtyByteMap = new DirtyByteMap(sheaf.getPageSize());
        this.sheaf = sheaf;
        this.position = position;
        this.lock = new ReentrantLock();
    }

    /**
     * Get the sheaf that manages this raw page.
     * 
     * @return The sheaf.
     */
    public Sheaf getSheaf() {
        return sheaf;
    }

    public Lock getLock() {
        return lock;
    }

    /**
     * Set the <code>Page</code> class that implements a specific type of page
     * that reads and writes to this underlying raw page.
     * <p>
     * This can only be set by the <code>Sheaf</code> that maintains the
     * <code>RawPage</code>.
     * 
     * @param page
     *            The <code>Page</code> class that reads and writes to this raw
     *            page.
     */
    void setPage(Page page) {
        this.page = page;
    }

    /**
     * Get the <code>Page</code> class that implements a specific type of page
     * that reads and writes to this underlying raw page.
     * 
     * @return The <code>Page</code> class that reads and writes to this raw
     *         page.
     */
    public Page getPage() {
        return page;
    }

    /**
     * Load the byte buffer from the file channel of the sheaf at the position
     * of this page offset by the offset of the sheaf.
     * 
     * @return A byte buffer of the contents of this page.
     */
    private ByteBuffer load() {
        int pageSize = sheaf.getPageSize();
        ByteBuffer bytes = ByteBuffer.allocateDirect(pageSize);
        try {
            sheaf.getFileChannel().read(bytes, sheaf.getOffset() + getPosition());
        } catch (IOException e) {
            throw new SheafException(103, e);
        }
        bytes.clear();

        return bytes;
    }

    /**
     * Returns the softly referenced byte buffer of the content of this raw
     * page. The byte buffer is read from the file channel of the sheaf at the
     * position of this page offset by the offset of the sheaf.
     * <p>
     * The byte buffer is lazily initialized, so that the byte buffer is read on
     * the first call to this method.
     * <p>
     * The byte buffer is also softly referenced so this method checks to see if
     * the byte buffer has been reclaimed and reads in a new byte buffer it has
     * been reclaimed.
     * <p>
     * Whenever we write to the byte buffer we hold onto a hard reference to the
     * byte buffer until the byte buffer is written back to file. We keep a hard
     * reference to the raw page and the byte buffer returned by this method in
     * the dirty page map. The dirty page map will flush the page.
     * 
     * @return A byte buffer of the contents of this page.
     */
    public synchronized ByteBuffer getByteBuffer() {
        ByteBuffer bytes = null;

        if (byteBufferReference == null) {
            bytes = load();
            byteBufferReference = new WeakReference<ByteBuffer>(bytes);
        } else {
            bytes = byteBufferReference.get();
            if (bytes == null) {
                bytes = load();
                byteBufferReference = new WeakReference<ByteBuffer>(bytes);
            }
        }

        return bytes;
    }

    /**
     * Write the dirty regions to the given file channel using the given disk at
     * the position of this dirty region map offset by the given offset.
     * 
     * @param fileChannel
     *            The file channel to write to.
     * @param offset
     *            An offset to add to the dirty region map file position.
     * @throws IOException
     *             If an I/O error occurs.
     */
    public void write(FileChannel fileChannel, int offset) throws IOException {
        dirtyByteMap.write(getByteBuffer(), fileChannel, getPosition() + offset);
    }

    /**
     * Get the length of raw page buffer.
     * 
     * @return The length of raw page buffer.
     */
    public int getLength() {
        return dirtyByteMap.getLength();
    }

    /**
     * Mark as dirty the bytes in the byte buffer starting at the given offset
     * and extending for the given length.
     * 
     * @param offset
     *            The offset of the dirty region.
     * @param length
     *            The length of the dirty region.
     */
    public void dirty(int offset, int length) {
        dirtyByteMap.dirty(offset, length);
    }

    /**
     * Mark as dirty the entire byte buffer.
     */
    public void dirty() {
        dirtyByteMap.dirty();
    }
}