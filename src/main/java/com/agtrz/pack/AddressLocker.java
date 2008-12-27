package com.agtrz.pack;

import java.util.SortedSet;

/**
 * This is a sorted set because for one strategy where addresses are keys
 * and reused, we only need to track a minimum and maximum. With little
 * consideration, the sorted set is helpful in that implementation.
 */
public interface AddressLocker
{
    public void lock(SortedSet<Long> setOfAddresses, Long address);
    
    public void unlock(SortedSet<Long> setOfAddresses);

    public void bide(Long address);
}