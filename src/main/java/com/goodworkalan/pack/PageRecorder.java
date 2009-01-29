package com.goodworkalan.pack;

import java.util.Set;

final class PageRecorder
extends CompositeMoveRecorder
{
    private final SetRecorder trackedUserPages;

    private final SetRecorder journalPages;
    
    private final SetRecorder writeBlockPages;
    
    private final SetRecorder allocBlockPages;
    
    public PageRecorder()
    {
        add(this.trackedUserPages = new SetRecorder());
        add(this.journalPages = new SetRecorder());
        add(this.writeBlockPages = new SetRecorder());
        add(this.allocBlockPages = new SetRecorder());
    }
    
    public Set<Long> getTrackedUserPages()
    {
        return trackedUserPages;
    }
    
    public Set<Long> getJournalPages()
    {
        return journalPages;
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
}