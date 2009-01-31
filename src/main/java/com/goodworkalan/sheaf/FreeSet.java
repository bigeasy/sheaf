package com.goodworkalan.sheaf;

import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

final class FreeSet
implements Iterable<Long>
{
    private final SortedSet<Long> setOfPositions;
    
    private final SortedSet<Long> setToIgnore;
    
    public FreeSet()
    {
        this.setOfPositions = new TreeSet<Long>();
        this.setToIgnore = new TreeSet<Long>();
    }
    
    public synchronized int size()
    {
        return setOfPositions.size();
    }
    
    public Iterator<Long> iterator()
    {
        return setOfPositions.iterator();
    }
    
    /**
     * Remove the interim page from the set of free interim pages if the
     * page is in the set of free interim pages. Returns true if the page
     * was in the set of free interim pages.
     *
     * @param position The position of the interim free page.
     */
    public synchronized boolean reserve(long position)
    {
        if (setOfPositions.remove(position))
        {
            return true;
        }
        setToIgnore.add(position);
        return false;
    }
    
    public synchronized void release(Set<Long> setToRelease)
    {
        setToIgnore.removeAll(setToRelease);
    }
    
    public synchronized long allocate()
    {
        if (setOfPositions.size() != 0)
        {
            long position = setOfPositions.first();
            setOfPositions.remove(position);
            return position;
        }
        return 0L;
    }
    
    public synchronized void free(Set<Long> setOfPositions)
    {
        for (long position : setOfPositions)
        {
            free(position);
        }
    }

    public synchronized void free(long position)
    {
        if (!setToIgnore.contains(position))
        {
            setOfPositions.add(position);
        }
    }
}