package com.goodworkalan.sheaf;

import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

/**
 * A container for outstanding <code>Page</code> objects that maps addresses to
 * soft referenced <code>Page</code> objects.
 */
public final class Sheaf
{
    /** An file channel opened on the associated file. */
    private final FileChannel fileChannel;
    
    /** The size of a page. */
    private final int pageSize;
    
    /** The offset of the first page. */
    private final int offset;

    /**
     * The map of weak references to raw pages keyed on the file position of the
     * raw page.
     */
    private final Map<Long, RawPageReference> rawPageByPosition;

    /**
     * The queue of weak references to raw pages keyed on the file position of
     * the raw page that is used to remove mappings from the map of pages by
     * position when the raw pages are collected.
     */
    private final ReferenceQueue<RawPage> queue;

    /**
     * Create a new sheaf the divides the given file channel into pages of the
     * given page size starting at the given offset.
     * <p>
     * The offset added to the positions of pages in the sheaf to get an actual
     * file position. The offset allows client programmers to use the start of
     * the file for header information, but still have nicely rounded page
     * positions beginning at page zero.
     * 
     * @param fileChannel
     *            An file channel opened on the associated file.
     * @param pageSize
     *            The size of a page in the sheaf.
     * @param offset
     *            The offset of the first page.
     */
    public Sheaf(FileChannel fileChannel, int pageSize, int offset)
    {
        this.fileChannel = fileChannel;
        this.pageSize = pageSize;
        this.offset = offset;
        this.rawPageByPosition = new HashMap<Long, RawPageReference>();
        this.queue = new ReferenceQueue<RawPage>();
    }

    /**
     * Return the file channel open on the underlying file.
     * 
     * @return The open file channel.
     */
    public FileChannel getFileChannel()
    {
        return fileChannel;
    }

    /**
     * Return the offset into the file where the first page is location. This
     * offset is added to the positions of {@link RawPage} instances to
     * determine the actual position of the raw page.
     * 
     * @return The offset of the first page.
     */
    public int getOffset()
    {
        return offset;
    }

    /**
     * Get the size of all underlying pages managed by this pager.
     * 
     * @return The page size.
     */
    public int getPageSize()
    {
        return pageSize;
    }

    /**
     * Remove the references to pages that have garbage collected from their
     * reference queue and from the map of raw pages by position.
     */
    private void collect()
    {
        synchronized (rawPageByPosition)
        {
            RawPageReference pageReference = null;
            while ((pageReference = (RawPageReference) queue.poll()) != null)
            {
                rawPageByPosition.remove(pageReference.getPosition());
            }
        }
    }

    /**
     * Allocate a new page from the end of the file by extending the length of
     * the file.
     *
     * @return The address of a new page from the end of file.
     */
    public long extend()
    {
        ByteBuffer bytes = ByteBuffer.allocateDirect(pageSize);

        bytes.putLong(0L); // Checksum.
        bytes.putInt(0); // Is system page.

        bytes.clear();

        long position;

        synchronized (this)
        {
            try
            {
                position = fileChannel.size();
            }
            catch (IOException e)
            {
                throw new SheafException(104, e);
            }
            
            if (position < offset)
            {
                position = offset;
            }

            try
            {
                fileChannel.write(bytes, position);
            }
            catch (IOException e)
            {
                throw new SheafException(101, e);
            }

            try
            {
                long size = fileChannel.size();
                if ((size - getOffset()) % pageSize != 0)
                {
                    throw new SheafException(105);
                }
            }
            catch (IOException e)
            {
                throw new SheafException(104, e);
            }
        }

        return position - offset;
    }

    /**
     * Add a raw page to the page by position map.
     *
     * @param rawPage The raw page.
     */
    private void addRawPageByPosition(RawPage rawPage)
    {
        RawPageReference intended = new RawPageReference(rawPage, queue);
        RawPageReference existing = rawPageByPosition.get(intended.getPosition());
        if (existing != null)
        {
            existing.enqueue();
            collect();
        }
        rawPageByPosition.put(intended.getPosition(), intended);
    }

    /**
     * Get a raw page from the map of pages by position. If the page reference
     * does not exist in the map of pages by position, or if the page reference
     * has been garbage collected, this method returns null.
     * 
     * @param position
     *            The position of the raw page.
     * @return The page currently mapped to the position or null if no page is
     *         mapped.
     */
    private RawPage getRawPageByPosition(long position)
    {
        RawPage page = null;
        RawPageReference chunkReference = rawPageByPosition.get(position);
        if (chunkReference != null)
        {
            page = chunkReference.get();
        }
        return page;
    }

    /**
     * Get a given page implementation of an underlying raw page for the given
     * position. If the page does not exist, the given page instance is used to
     * load the contents of the underlying raw page. Creation of the page is
     * synchronized so that all mutators will reference the same instance of
     * <code>Page</code> and <code>RawPage</code>.
     * <p>
     * If the given page class is a subclass of the page instance currently
     * mapped to the page position, the given page is used to load the contents
     * of the underlying raw page and the current page instance is replaced with
     * the subclass page instance. This is used to upgrade a relocatable page,
     * to a specific type of relocatable page (journal, user block, or interim
     * block).
     * <p>
     * If the given page class is a superclass of the page instance currently
     * mapped to the page position, the current page is returned.
     * <p>
     * The page instance is one that is created solely for this invocation of
     * <code>getPage</code>. It a page of the correct type is in the map of
     * pages by position, the given page instance is ignored and left for the
     * garbage collector.
     * <p>
     * The given page class is nothing more than a type token, to cast the page
     * to correct page type, without generating unchecked cast compiler
     * warnings.
     * 
     * @param position
     *            The page position.
     * @param pageClass
     *            A type token indicating the type of page, used to cast the
     *            page.
     * @param page
     *            An instance to used to load the page if the page does not
     *            exist in the page map.
     * @return The page of the given type for the given position.
     */
    public <P extends Page> P getPage(long position, Class<P> pageClass, P page)
    {
        position = (long) Math.floor(position - (position % pageSize));
        RawPage rawPage = new RawPage(this, position);
        synchronized (rawPage)
        {
            RawPage found = null;
            synchronized (rawPageByPosition)
            {
                found = getRawPageByPosition(position);
                if (found == null)
                {
                    addRawPageByPosition(rawPage);
                }
            }
            if (found == null)
            {
                page.setRawPage(rawPage);
                rawPage.setPage(page);
                page.load();
            }
            else
            {
                rawPage = found;
            }
        }
        synchronized (rawPage)
        {
            if (!page.getClass().isAssignableFrom(rawPage.getPage().getClass()))
            {
                if (!rawPage.getPage().getClass().isAssignableFrom(page.getClass()))
                {
                    throw new IllegalStateException();
                }
                page.setRawPage(rawPage);
                rawPage.setPage(page);
                page.load();
            }
        }
        return pageClass.cast(rawPage.getPage());
    }

    /**
     * Set the page at the given position in the map of raw pages by position,
     * to the given page class and given page. This method is called after a
     * page has been moved and its type has been changed, from user block page
     * to address page, or from interim page to user block page.
     * 
     * @param position
     *            The page position.
     * @param pageClass
     *            A type token indicating the type of page, used to cast the
     *            page.
     * @param page
     *            An instance to used to load the page if the page does not
     *            exist in the page map.
     * @param dirtyPages
     *            The set of dirty pages.
     * @param extant
     *            If false, the method will assert that the page does not
     *            already exist in the map of pages by position.
     * @return The page given.
     */
    public <P extends Page> P setPage(long position, Class<P> pageClass, P page, DirtyPageSet dirtyPages)
    {
        position =  position / pageSize * pageSize;
        RawPage rawPage = new RawPage(this, position);

        synchronized (rawPageByPosition)
        {
            RawPage existing = removeRawPageByPosition(position);
            if (existing != null)
            {
                throw new IllegalStateException();
            }
            else
            {
                page.setRawPage(rawPage);
                rawPage.setPage(page);
                page.create(dirtyPages);
                addRawPageByPosition(rawPage);
            }
        }

        return pageClass.cast(rawPage.getPage());
    }
    
    // TODO Document.
    public void free(long position)
    {
        synchronized (rawPageByPosition)
        {
            removeRawPageByPosition(position);
        }
    }

    // TODO Document.
    private void copy(RawPage rawPage, long to)
    {
        ByteBuffer bytes = rawPage.getByteBuffer();
        bytes.clear();
        try
        {
            getFileChannel().write(bytes, to);
        }
        catch (IOException e)
        {
            throw new SheafException(0, e);
        }
        rawPage.setPosition(to);
    }

    // TODO Document.
    public void move(long from, long to)
    {
        RawPage rawPage = null;
        synchronized (rawPageByPosition)
        {
            if (getRawPageByPosition(to) != null)
            {
                throw new IllegalStateException();
            }
            rawPage = getRawPageByPosition(from);
            if (rawPage == null)
            {
                copy(new RawPage(this, from), to);
            }
        }
        synchronized (rawPage)
        {
            synchronized (rawPageByPosition)
            {
                if (getRawPageByPosition(to) != null)
                {
                    throw new IllegalStateException();
                }
                removeRawPageByPosition(from);
                copy(new RawPage(this, from), to);
                addRawPageByPosition(rawPage);
            }
            rawPage.setPosition(to);
        }
    }

    /**
     * Remove a raw page from the map of pages by position. If the page exists
     * in the map, The page is completely removed by queuing the weak page
     * reference and running <code>collect</code>.
     * 
     * @param position
     *            The position of the raw page to remove.
     * @return The page currently mapped to the position or null if no page is
     *         mapped.
     */
    private RawPage removeRawPageByPosition(long position)
    {
        RawPageReference existing = rawPageByPosition.get(new Long(position));
        RawPage p = null;
        if (existing != null)
        {
            p = existing.get();
            existing.enqueue();
            collect();
        }
        return p;
    }

    /**
     * Relocate a page in the pager by removing it from the map of pages by
     * position at the given from position and adding it at the given to
     * position. This only moves the <code>RawPage</code> in the pager, it does
     * not copy the page to the new position in the underlying file.
     * 
     * @param from
     *            The position to move from.
     * @param to
     *            The position to move to.
     */
    public void relocate(long from, long to)
    {
        synchronized (rawPageByPosition)
        {
            RawPage position = removeRawPageByPosition(from);
            if (position != null)
            {
                assert to == position.getPosition();
                addRawPageByPosition(position);
            }
        }
    }
}

/* vim: set tw=80 : */
