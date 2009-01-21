package com.goodworkalan.pack;

import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;


final class Commit
extends CompositeMoveRecorder
{
    private final MapRecorder vacuumMap;
    
    private final MapRecorder emptyMap;
    
    private final SortedSet<Long> addressSet;
    
    private final SortedSet<Long> userFromInterimPages;
    
    private final SortedSet<Long> addressFromUserPagesToMove;
    
    private final SetRecorder unassignedInterimBlockPages;
    
    private final SortedMap<Long, Movable> movingUserPageMirrors;
    
    public Commit(PageRecorder pageRecorder, Journal journal, MoveNodeRecorder moveNodeRecorder)
    {
        this.addressSet = new TreeSet<Long>();
        this.userFromInterimPages = new TreeSet<Long>();
        this.addressFromUserPagesToMove = new TreeSet<Long>();
        this.movingUserPageMirrors = new TreeMap<Long, Movable>();
        add(unassignedInterimBlockPages = new SetRecorder());
        add(pageRecorder);
        add(vacuumMap = new MapRecorder());
        add(emptyMap = new MapRecorder());
        add(moveNodeRecorder);
        add(new JournalRecorder(journal));
    }
    
    @Override
    public boolean involves(long position)
    {
        return addressSet.contains(position)
            || super.involves(position);
    }
    
    public boolean isAddressExpansion()
    {
        return addressSet.size() != 0;
    }

    public SortedSet<Long> getAddressSet()
    {
        return addressSet;
    }

    /**
     * A set of positions of user pages that have been created during the commit
     * by relocating interim pages and expanding the user page region. These
     * pages were interim pages at the start of this commit.
     * 
     * @return A set of positions of newly created user pages.
     */
    public SortedSet<Long> getUserFromInterimPages()
    {
        return userFromInterimPages;
    }
    
    public SortedMap<Long, Movable> getMovingUserPageMirrors()
    {
        return movingUserPageMirrors;
    }
    
    public SortedSet<Long> getAddressFromUserPagesToMove()
    {
        return addressFromUserPagesToMove;
    }

    /**
     * Return the set of allocation pages whose blocks have not yet been
     * assigned to a user block page.
     * 
     * @return The set of unassigned interim blocks.
     */
    public SortedSet<Long> getUnassignedInterimBlockPages()
    {
        return unassignedInterimBlockPages;
    }

    public SortedMap<Long, Movable> getVacuumMap()
    {
        return vacuumMap;
    }
    
    public SortedMap<Long, Movable> getEmptyMap()
    {
        return emptyMap;
    }
}