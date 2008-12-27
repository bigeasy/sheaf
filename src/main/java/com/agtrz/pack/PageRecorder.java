package com.agtrz.pack;

import java.util.HashSet;
import java.util.Set;

final class PageRecorder
extends CompositeMoveRecorder
{
    private final Set<Long> setOfAddressPages;

    private final SetRecorder setOfUserPages;

    private final SetRecorder setOfJournalPages;
    
    private final SetRecorder setOfWritePages;
    
    private final SetRecorder setOfAllocationPages;
    
    public PageRecorder()
    {
        this.setOfAddressPages = new HashSet<Long>();
        add(this.setOfUserPages = new SetRecorder());
        add(this.setOfJournalPages = new SetRecorder());
        add(this.setOfWritePages = new SetRecorder());
        add(this.setOfAllocationPages = new SetRecorder());
    }
    
    public Set<Long> getAddressPageSet()
    {
        return setOfAddressPages;
    }
    
    public Set<Long> getUserPageSet()
    {
        return setOfUserPages;
    }
    
    public Set<Long> getJournalPageSet()
    {
        return setOfJournalPages;
    }
    
    public Set<Long> getWritePageSet()
    {
        return setOfWritePages;
    }
    
    public Set<Long> getAllocationPageSet()
    {
        return setOfAllocationPages;
    }
    
    public void clear()
    {
        setOfAddressPages.clear();
        super.clear();
    }
}