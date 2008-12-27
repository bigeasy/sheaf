package com.agtrz.pack;

import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;


final class Commit
extends CompositeMoveRecorder
{
    private final MapRecorder mapOfVaccums;
    
    private final MapRecorder mapOfEmpties;
    
    private final SortedSet<Long> setOfAddressPages;
    
    private final SortedSet<Long> setOfGatheredPages;
    
    private final SortedSet<Long> setOfInUseAddressPages;
    
    private final SetRecorder setOfUnassigned;
    
    private final SortedMap<Long, Movable> mapOfAddressMirrors;
    
    public Commit(PageRecorder pageRecorder, Journal journal, MoveNodeRecorder moveNodeRecorder)
    {
        this.setOfAddressPages = new TreeSet<Long>();
        this.setOfGatheredPages = new TreeSet<Long>();
        this.setOfInUseAddressPages = new TreeSet<Long>();
        this.mapOfAddressMirrors = new TreeMap<Long, Movable>();
        add(setOfUnassigned = new SetRecorder());
        add(pageRecorder);
        add(mapOfVaccums = new MapRecorder());
        add(mapOfEmpties = new MapRecorder());
        add(moveNodeRecorder);
        add(new JournalRecorder(journal));
    }
    
    @Override
    public boolean involves(long position)
    {
        return setOfAddressPages.contains(position)
            || super.involves(position);
    }
    
    public boolean isAddressExpansion()
    {
        return setOfAddressPages.size() != 0;
    }

    public SortedSet<Long> getAddressSet()
    {
        return setOfAddressPages;
    }
    
    public SortedSet<Long> getGatheredSet()
    {
        return setOfGatheredPages;
    }
    
    public SortedMap<Long, Movable> getAddressMirrorMap()
    {
        return mapOfAddressMirrors;
    }
    
    public SortedSet<Long> getInUseAddressSet()
    {
        return setOfInUseAddressPages;
    }
    
    public SortedSet<Long> getUnassignedSet()
    {
        return setOfUnassigned;
    }

    public SortedMap<Long, Movable> getVacuumMap()
    {
        return mapOfVaccums;
    }
    
    public SortedMap<Long, Movable> getEmptyMap()
    {
        return mapOfEmpties;
    }
}