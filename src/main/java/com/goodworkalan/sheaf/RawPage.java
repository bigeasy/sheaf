package com.goodworkalan.sheaf;

import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;

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
 * which maintains a concurrency lock in the data page object. See
 * {@link BlockPage} for more details on the separate soft references.
 */
public final class RawPage extends Invalidator
{
    /** The pager that manages this raw page. */
    private final Sheaf pager;

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
     * Create a raw page at the specified position associated with the specified
     * pager. The byte buffer is not loaded until the {@link #getByteBuffer
     * getByteBuffer} method is called.
     * 
     * @param pager
     *            The pager that manages this raw page.
     * @param position
     *            The position of the page.
     */
    public RawPage(Sheaf pager, long position)
    {
        super(position);
        this.pager = pager;
    }

    /**
     * Get the pager that manages this raw page.
     *
     * @return The pager.
     */
    public Sheaf getPager()
    {
        return pager;
    }

    /**
     * Set the <code>Page</code> class that implements a specific type of page
     * that reads and writes to this underlying raw page.
     * 
     * @param page
     *            The <code>Page</code> class that reads and writes to this raw
     *            page.
     */
    public void setPage(Page page)
    {
        this.page = page;
    }

    /**
     * Get the <code>Page</code> class that implements a specific type of page
     * that reads and writes to this underlying raw page.
     * 
     * @return The <code>Page</code> class that reads and writes to this raw
     *         page.
     */
    public Page getPage()
    {
        return page;
    }

    /**
     * Load the byte buffer from the file channel of the pager.
     * 
     * @return A byte buffer of the contents of this page.
     */
    private ByteBuffer load()
    {
        int pageSize = pager.getPageSize();
        ByteBuffer bytes = ByteBuffer.allocateDirect(pageSize);
        try
        {
            pager.getDisk().read(pager.getFileChannel(), bytes, getPosition());
        }
        catch (IOException e)
        {
            throw new SheafException(103, e);
        }
        bytes.clear();

        return bytes;
    }

    /**
     * Returns the softly referenced byte buffer of the content of this raw
     * page.
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
    public synchronized ByteBuffer getByteBuffer()
    {
        ByteBuffer bytes = null;

        if (byteBufferReference == null)
        {
            bytes = load();
            byteBufferReference = new WeakReference<ByteBuffer>(bytes);
        }
        else
        {
            bytes = byteBufferReference.get();
            if (bytes == null)
            {
                bytes = load();
                byteBufferReference = new WeakReference<ByteBuffer>(bytes);
            }
        }

        return bytes;
    }
}