package com.goodworkalan.sheaf;

import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

/**
 * An iterator over the page positions of a {@link ByRemainingTable} that iterates in
 * the ascending order of the amount of bytes remaining for block allocation.
 * The iterator will return the a page positions grouped by the amount of bytes
 * remaining, in the ascending order of the amount of bytes remaining.
 * 
 * @author Alan Gutierrez
 */
final class ByRemainingTableIterator implements Iterator<Long>
{
    /** An iterator over a list of sets of pages by amount remaining. */
    private Iterator<SortedSet<Long>> pagesBySize;
    
    /** An iterator of a set of pages in the array of pages by remaining. */
    private Iterator<Long> pages;

    /**
     * Create an iterator over an array of sets of pages where the index of the
     * set of pages for a given page is determined by rounding down the amount
     * of bytes remaining for allocation down to the nearest alignment and
     * dividing that by the alignment.
     * 
     * @param pagesBySize
     *            The array of sets of pages by remaining.
     */
    public ByRemainingTableIterator(List<SortedSet<Long>> pagesBySize)
    {
        Iterator<SortedSet<Long>> bySize = pagesBySize.iterator();
        Iterator<Long> pages = bySize.next().iterator();
        while (!pages.hasNext() && bySize.hasNext())
        {
            pages = bySize.next().iterator();
        }
        this.pagesBySize = bySize;
        this.pages = pages;
    }

    /**
     * Return true if there are more page positions to iterate.
     * 
     * @return If there are more page positions.
     */
    public boolean hasNext()
    {
        return pages.hasNext() || pagesBySize.hasNext();
    }
    
    /**
     * Return the next page position in ascending order of bytes remaining.
     * 
     * @return The next page position.
     */
    public Long next()
    {
        while (!pages.hasNext())
        {
            if (!pagesBySize.hasNext())
            {
                throw new ArrayIndexOutOfBoundsException();
            }
            pages = pagesBySize.next().iterator();
        }
        long size = pages.next();
        while (!pages.hasNext() && pagesBySize.hasNext())
        {
            pages = pagesBySize.next().iterator();
        }
        return size;
    }
    
    /**
     * The remove method is unsupported by this iterator.
     * 
     * @throws UnsupportedOperationException If invoked.
     */
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}