package com.goodworkalan.pack;

import java.util.HashSet;
import java.util.Set;

final class PageRecorder
extends CompositeMoveRecorder
{
    private final Set<Long> addressPageSet;

    private final SetRecorder userPageSet;

    private final SetRecorder journalPageSet;
    
    private final SetRecorder writeBlockPages;
    
    private final SetRecorder allocBlockPages;
    
    public PageRecorder()
    {
        this.addressPageSet = new HashSet<Long>();
        add(this.userPageSet = new SetRecorder());
        add(this.journalPageSet = new SetRecorder());
        add(this.writeBlockPages = new SetRecorder());
        add(this.allocBlockPages = new SetRecorder());
    }
    
    public Set<Long> getAddressPageSet()
    {
        return addressPageSet;
    }
    
    public Set<Long> getUserPageSet()
    {
        return userPageSet;
    }
    
    public Set<Long> getJournalPageSet()
    {
        return journalPageSet;
    }

    /**
     * Return a set of interim block pages used to store blocks that represent a
     * write to an existing block in a user block page.
     * 
     * @return The set of interim block pages containing writes.
     */
    public Set<Long> getWriteBlockPages()
    {
        return writeBlockPages;
    }

    /**
     * Return a set of interim block pages used to store newly allocated blocks
     * that do not yet have a block in a user block page.
     * 
     * @return The set of interim block pages containing block allocations.
     */
   public Set<Long> getAllocBlockPages()
    {
        return allocBlockPages;
    }
    
    public void clear()
    {
        addressPageSet.clear();
        super.clear();
    }
}