package com.goodworkalan.sheaf;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Used during commit to lock freed addresses to prevent their possible
 * reallocation from being written before the free operation commit completes.
 * <p>
 * This class is specific to the relatively simple task of preventing the
 * overwrite of a reallocation by replaying a journaled free. It is not intended
 * to be the basis of a general purpose synchronization algorithm. Specifically,
 * it is not intended to lock addresses for users, to coordinate user writes or
 * frees.
 * <p>
 * During free, the address in the address page is set to zero, indicating that
 * the address is available for reallocation. Without some form of
 * synchronization it is possible for another mutator to reallocate the freed
 * address, then commit the reallocation, before the commit that freed the
 * address completes. If the commit that freed the address were not to complete
 * due to a system failure, then when the journals were replayed during recovery
 * the reallocation would be overwritten by the replay of the free.
 * <p>
 * This implementation multiplexes the addresses in 37 different sets of long
 * values and a set is chosen by hashing. Synchronization is performed on the
 * set, so contention is reduced by the reduced chance of two addresses hashing
 * to the same set at the same time.
 * 
 * @author Alan Gutierrez
 */
class AddressLocker
{
    /** A list of 37 sets of locked addresses. */
    private final List<Set<Long>> lockedAddressSets;
    
    /**
     * Create the address locker. There is one address locker per pager.
     */
    public AddressLocker()
    {
        List<Set<Long>> lockedAddressSets = new ArrayList<Set<Long>>(37);
        for (int i = 0; i < 37; i++)
        {
            lockedAddressSets.add(new HashSet<Long>());
        }
        this.lockedAddressSets = lockedAddressSets;
    }

    /**
     * Lock the given address, adding it to a set of locked addresses, so that a
     * call to the <code>bide</code> method will block until the the address is
     * unlocked using the <code>unlock</code> method.
     * 
     * @param address
     *            The address to lock.
     */
    public void lock(Long address)
    {
        Set<Long> lockedAddresses = lockedAddressSets.get(address.hashCode() % 37);
        synchronized (lockedAddresses)
        {
            assert ! lockedAddresses.contains(address);
            lockedAddresses.add(address);
        }
    }

    /**
     * Unlock all the addresses in the given set of addresses, removing them
     * from this set of locked addresses, and notifying all threads waiting for
     * the address in the <code>bide</code> method.
     * 
     * @param addresses
     *            The set of addresses to unlock.
     */
    public void unlock(Set<Long> addresses)
    {
        for (Long address : addresses)
        {
            Set<Long> lockedAddresses = lockedAddressSets.get(address.hashCode() % 37);
            synchronized (lockedAddresses)
            {
                assert lockedAddresses.contains(address);
                lockedAddresses.remove(address);
                lockedAddresses.notifyAll();
            }
        }
    }

    /**
     * If the given address is in the set of locked addresses, block the current
     * thread waiting for the address to become unlocked.
     * 
     * @param address
     *            The address to check.
     */
    public void bide(Long address)
    {
        Set<Long> lockedAddresses = lockedAddressSets.get(address.hashCode() % 37);
        synchronized (lockedAddresses)
        {
            while (lockedAddresses.contains(address))
            {
                try
                {
                    lockedAddresses.wait();
                }
                catch (InterruptedException e)
                {
                }
            }
        }
    }
}
