package com.goodworkalan.pack;

import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

final class Commit extends CompositeMoveRecorder
{
    /**
     * A map of interim pages containing new allocations to destination user
     * blocks pages that already contain blocks from other mutators.
     */
    private final MapRecorder interimToSharedUserPage;

    /**
     * A map of interim pages containing new allocations to destination user
     * blocks pages that contain no blocks from other mutators.
     */
    private final MapRecorder interimToEmptyUserPage;

    private final SortedSet<Long> userFromInterimPages;

    private final SortedSet<Long> addressFromUserPagesToMove;

    private final SetRecorder unassignedInterimBlockPages;

    private final SortedMap<Long, Movable> movingUserPageMirrors;

    public Commit(PageRecorder pageRecorder, Journal journal,
            MoveNodeRecorder moveNodeRecorder)
    {
        this.userFromInterimPages = new TreeSet<Long>();
        this.addressFromUserPagesToMove = new TreeSet<Long>();
        this.movingUserPageMirrors = new TreeMap<Long, Movable>();
        add(unassignedInterimBlockPages = new SetRecorder());
        add(pageRecorder);
        add(interimToSharedUserPage = new MapRecorder());
        add(interimToEmptyUserPage = new MapRecorder());
        add(moveNodeRecorder);
        add(new JournalRecorder(journal));
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

    /**
     * Return a map of interim pages containing new allocations to destination
     * user blocks pages that are already in use and contain user blocks
     * allocated by other mutators.
     * 
     * @return The map of shared user pages.
     */
    public SortedMap<Long, Movable> getInterimToSharedUserPage()
    {
        return interimToSharedUserPage;
    }

    /**
     * Return a map of interim pages containing new allocations to destination
     * user blocks pages that are not currently in use and contain no blocks
     * from other mutators.
     * 
     * @return The map of empty user pages.
     */
    public SortedMap<Long, Movable> getInterimToEmptyUserPage()
    {
        return interimToEmptyUserPage;
    }
}