package com.goodworkalan.pack;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

/**
 * Used during commit to lock freed addresses to prevent their possible
 * reallocation from being written before the free operation commit completes.
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
 * values and a set is chosen by hasing. Synchronization is performed on the
 * set, so contention is reduced by the reduced chance of two addresses hashing
 * to the same set at the same time.
 * 
 * @author Alan Gutierrez
 */
class AddressLocker
{
    private final List<Set<Long>> listOfSetsOfAddress;
    
    public AddressLocker()
    {
        List<Set<Long>> listOfSetsOfAddressess = new ArrayList<Set<Long>>(37);
        for (int i = 0; i < 37; i++)
        {
            listOfSetsOfAddressess.add(new HashSet<Long>());
        }
        this.listOfSetsOfAddress = listOfSetsOfAddressess;
    }
    
    public void lock(SortedSet<Long> setOfAddresses, Long address)
    {
        Set<Long> setOfLockedAddresses = listOfSetsOfAddress.get(address.hashCode() % 37);
        synchronized (setOfLockedAddresses)
        {
            assert ! setOfLockedAddresses.contains(address);
            setOfLockedAddresses.add(address);
        }
        setOfAddresses.add(address);
    }

    public void unlock(SortedSet<Long> setOfAddresses)
    {
        for (Long address : setOfAddresses)
        {
            Set<Long> setOfLockedAddresses = listOfSetsOfAddress.get(address.hashCode() % 37);
            synchronized (setOfLockedAddresses)
            {
                assert setOfLockedAddresses.contains(address);
                setOfLockedAddresses.remove(address);
                setOfLockedAddresses.notifyAll();
            }
        }
    }

    public void bide(Long address)
    {
        Set<Long> setOfLockedAddresses = listOfSetsOfAddress.get(address.hashCode() % 37);
        synchronized (setOfLockedAddresses)
        {
            while (setOfLockedAddresses.contains(address))
            {
                try
                {
                    setOfLockedAddresses.wait();
                }
                catch (InterruptedException e)
                {
                }
            }
        }
    }
}