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

    /**
     * Wrapper interface to file channel to allow for IO error
     * simulation during testing.
     */
    private final Disk disk;
    
    /**
     * This size of a page.
     */
    private final int pageSize;
    
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
     * Create a new pager.
     * 
     * @param fileChannel
     *            An file channel opened on the associated file.
     * @param disk
     *            Wrapper interface to file channel to allow for IO error
     *            simulation during testing.
     * @param pageSize
     *            The size of a page in the sheaf.
     */
    public Sheaf(FileChannel fileChannel, Disk disk, int pageSize, int offset)
    {
        this.fileChannel = fileChannel;
        this.disk = disk;
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
    
    public int getOffset()
    {
        return offset;
    }

    /**
     * Return the disk object used to read and write to the file channel. The
     * disk is an class that can be overridden to generate I/O errors during
     * testing.
     * 
     * @return The disk.
     */
    public Disk getDisk()
    {
        return disk;
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
    private synchronized void collect()
    {
        RawPageReference pageReference = null;
        while ((pageReference = (RawPageReference) queue.poll()) != null)
        {
            rawPageByPosition.remove(pageReference.getPosition());
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

        synchronized (disk)
        {
            try
            {
                position = disk.size(fileChannel);
            }
            catch (IOException e)
            {
                throw new SheafException(104, e);
            }

            try
            {
                disk.write(fileChannel, bytes, position);
            }
            catch (IOException e)
            {
                throw new SheafException(101, e);
            }

            try
            {
                if (disk.size(fileChannel) % 1024 != 0)
                {
                    throw new SheafException(105);
                }
            }
            catch (IOException e)
            {
                throw new SheafException(104, e);
            }
        }

        return position;
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
                page.load(rawPage);
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
                page.load(rawPage);
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
    public <P extends Page> P setPage(long position, Class<P> pageClass, P page, DirtyPageSet dirtyPages, boolean extant)
    {
        position =  position / pageSize * pageSize;
        RawPage rawPage = new RawPage(this, position);

        synchronized (rawPageByPosition)
        {
            RawPage existing = removeRawPageByPosition(position);
            if (existing != null)
            {
                if (!extant)
                {
                    throw new IllegalStateException();
                }
                // TODO Not sure about this. Maybe lock on existing?
                synchronized (existing)
                {
                    page.create(existing, dirtyPages);
                }
            }
            else
            {
                page.create(rawPage, dirtyPages);
                addRawPageByPosition(rawPage);
            }
        }

        return pageClass.cast(rawPage.getPage());
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
    
    public void force()
    {
        try
        {
            disk.force(fileChannel);
        }
        catch (IOException e)
        {
            throw new SheafException(102, e);
        }
    }
}

/* vim: set tw=80 : */
