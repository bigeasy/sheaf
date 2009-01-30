package com.goodworkalan.pack;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * A table to lookup pages by the amount of bytes remaining for block allocation.
 * The table rounds the amount remaining in a given page down to the nearest block alignment, then stores it 
 * in a set for that value. Use the {@link #bestFit(int)} method to find a page
 * that will fit a given block size. Use {@link #reserve(long)} to prevent
 * a page from being returned 
 * 
 * A table of pages ordered by size that performs a best fit lookup, returning
 * the page in the collection with the least amount of free space that will
 * accommodate a block of a given size.
 * 
 * @author Alan Gutierrez
 */
final class ByRemainingTable implements Iterable<Long>
{
    /** The block alignment. */
    private final int alignment;
    
    /** A map of page position to amount remaining for reverse lookup. */
    private final Map<Long, Integer> pageToRemaining;

    /**
     * An array of sets of pages indexed by the aligned amount of bytes
     * remaining for block allocation.
     */
    private final List<SortedSet<Long>> pagesByRemaining;
    
    /** A map of pages to ignore if they are returned to this table. */
    private final SortedMap<Long, Integer> ignore;
    
    /** A map of pages to bytes remaining that are being ignored. */
    private final Map<Long, Integer> ignored;

    /**
     * Create a table to lookup pages by the amount of bytes remaining for block
     * allocation. The table will create sets of pages in a lookup table. The
     * index of to use to lookup a page is determined by rounding a block size
     * down to the nearest alignment, then dividing that number by the
     * alignment. The lookup table will have @(pageSize / alignment)@ slots,
     * each containing a page set.
     * 
     * @param pageSize
     *            The page size.
     * @param alignment
     *            The block alignment.
     */
    public ByRemainingTable(int pageSize, int alignment)
    {
        assert pageSize % alignment == 0;

        ArrayList<SortedSet<Long>> listOfSetsOfPages = new ArrayList<SortedSet<Long>>(pageSize / alignment);

        for (int i = 0; i < pageSize / alignment; i++)
        {
            listOfSetsOfPages.add(new TreeSet<Long>());
        }

        this.alignment = alignment;
        this.pagesByRemaining = listOfSetsOfPages;
        this.ignore = new TreeMap<Long, Integer>();
        this.pageToRemaining = new HashMap<Long, Integer>();
        this.ignored = new HashMap<Long, Integer>();
    }
    
    /**
     * Get the number of page positions currently tracked by this table.
     * 
     * @return The size of the table.
     */
    public synchronized int getSize()
    {
        return pageToRemaining.size();
    }

    /**
     * Return true if the table contains the given page position.
     * 
     * @param position
     *            The page position.
     * @return True if the table contains the position.
     */
    public synchronized boolean contains(long position)
    {
        return pageToRemaining.containsKey(position);
    }

    /**
     * Return an iterator over the page positions in the table that iterates in
     * the ascending order of the amount of bytes remaining for block
     * allocation. The iterator will return the a page positions grouped by the
     * amount of bytes remaining, in the ascending order of the amount of bytes
     * remaining.
     * 
     * @return An iterator over the page positions that iterates in ascending
     *         order of bytes remaining.
     */
    public Iterator<Long> iterator()
    {
        return new ByRemainingTableIterator(pagesByRemaining);
    }

    /**
     * Add the page position and amount of bytes remaining for block allocation
     * of the given block page to the table. If the amount of bytes remaining
     * for block allocation rounds down to zero, the page position is not added
     * to the table.
     * <p>
     * If the page position of the block has been reserved, the page will be
     * put into a waiting state until the page has been released, at which
     * point it will be added.
     * 
     * @param blocks
     *            The block page to add.
     */
    public synchronized void add(BlockPage blocks)
    {
        add(blocks.getRawPage().getPosition(), blocks.getRemaining());
    }

    /**
     * Add the page at the given position with the given amount of bytes
     * remaining for block allocation to the table. If the amount of bytes
     * remaining for block allocation rounds down to zero, the page position is
     * not added to the table.
     * <p>
     * If the page position has been reserved, the page will be put into a
     * waiting state until the page has been released, at which point it will be
     * added.
     * 
     * @param position
     *            The page position.
     * @param remaining
     *            The count of bytes remaining for block allocation.
     */
    public synchronized void add(long position, int remaining)
    {
        if (ignore.containsKey(position))
        {
            ignored.put(position, remaining);
        }
        else
        {
            int aligned = remaining / alignment * alignment;
            if (aligned != 0)
            {
                pageToRemaining.put(position, remaining);
                pagesByRemaining.get(aligned / alignment).add(position);
            }
        }
    }
    
    /**
     * Returns the amount of blocks remaining rounded down to the nearest
     * alignment.
     * 
     * @param position
     *            The page position.
     * @return The amount of blocks remaining rounded down to the nearest
     *         alignment.
     */
    public synchronized int getRemaining(long position)
    {
        Integer remaining = pageToRemaining.get(position);
        if (remaining != null)
        {
            return remaining;
        }
        return 0;
    }

    /**
     * Remove the given page position from the table.
     * <p>
     * Returns the amount of blocks remaining rounded down to the nearest
     * alignment. This is used by the {@link ByRemainingTableRecorder} to relocate
     * the page and amount remaining in the table, when the page moves.
     * 
     * @param position
     *            The page position.
     * @return The amount of blocks remaining rounded down to the nearest
     *         alignment.
     */
    public synchronized int remove(long position)
    {
        Integer remaining = pageToRemaining.get(position);
        if (remaining != null)
        {
            pageToRemaining.remove(position);
            int aligned = remaining / alignment * alignment;
            Set<Long> pages = pagesByRemaining.get(aligned / alignment);
            pages.remove(position);
            return remaining;
        }
        return 0;
    }

    /**
     * Tell the table to remove the given given page position and ignore it if
     * it is added to the table.
     * <p>
     * This is used to prevent a page from being returned to the table by a
     * mutator, after a mutator has scheduled the page to move, but has not yet
     * moved it.
     * 
     * @param position
     *            The page position.
     * @return True if the page existed in the table.
     */
    public synchronized boolean reserve(long position)
    {
        int remaining = remove(position);
        Integer count = ignore.get(position);
        ignore.put(position, count == null ? 1 : count + 1);
        return remaining != 0;
    }

    /**
     * Tell the table to stop ignoring the page at the given position and if it
     * has been added back to the table, to add back at the new position.
     * <p>
     * The new position may in some cases be the same as the position.
     * 
     * @param position
     *            The position to stop ignoring.
     * @param newPosition
     *            The new location of the position.
     */
    public synchronized void release(long position, long newPosition)
    {
        Integer count = ignore.remove(position);
        if (count == 1)
        {
            Integer remaining = ignored.remove(position);
            if (remaining != null)
            {
                add(newPosition, remaining);
            }
        }
        else
        {
            ignore.put(position, count - 1);
        }
    }

    /**
     * Return the page with the least amount of bytes remaining for allocation
     * that will fit the full block size. The block given block size must
     * include the block header.
     * <p>
     * The method will ascend the table looking at the slots for each remaining
     * size going form smallest to largest and returning the first to fit the
     * block, or null if no page can fit the block.
     * 
     * @param blockSize
     *            The block size including the block header.
     * @return A size object containing the a page that will fit the block or
     *         null if none exists.
     */
    public synchronized long bestFit(int blockSize)
    {
        long bestFit = 0L;
        int aligned = ((blockSize | alignment - 1) + 1); // Round up.
        if (aligned != 0)
        {
            for (int i = aligned / alignment; bestFit == 0L && i < pagesByRemaining.size(); i++)
            {
                if (!pagesByRemaining.get(i).isEmpty())
                {
                    bestFit = pagesByRemaining.get(i).first();
                    pagesByRemaining.get(i).remove(bestFit);
                    pageToRemaining.remove(bestFit);
                }
            }
        }
        return bestFit;
    }
    
    /**
     * Clear the table, removing all pages and ignores.   
     */
    public void clear()
    {
        for (SortedSet<Long> pages : pagesByRemaining)
        {
            pages.clear();
        }
        pageToRemaining.clear();
        ignore.clear();
    }
}
