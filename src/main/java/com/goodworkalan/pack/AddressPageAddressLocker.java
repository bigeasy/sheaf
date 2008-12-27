package com.goodworkalan.pack;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

class AddressPageAddressLocker
implements AddressLocker
{
    private final List<Set<Long>> listOfSetsOfAddress;
    
    public AddressPageAddressLocker()
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