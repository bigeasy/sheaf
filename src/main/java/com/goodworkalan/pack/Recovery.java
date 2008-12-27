package com.goodworkalan.pack;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.Adler32;
import java.util.zip.Checksum;


final class Recovery
{
    private final Pager pager;
    
    private final Checksum checksum;

    private int blockCount;

    private final Set<Long> setOfBadAddressChecksums;

    private final Map<Long, Map<Long, Long>> mapOfBadAddressPages;
    
    private final Set<Long> setOfCorruptDataPages;
    
    private final Set<Long> setOfBadUserChecksums;

    private final Map<Long, List<Long>> mapOfBadUserAddresses;
    
    private boolean addressRecovery;
    
    private long fileSize;
    
    public Recovery(Pager pager, long fileSize, boolean addressRecovery)
    {
        this.pager = pager;
        this.checksum = new Adler32();
        this.mapOfBadAddressPages = new HashMap<Long, Map<Long, Long>>();
        this.setOfBadAddressChecksums = new HashSet<Long>();
        this.setOfBadUserChecksums = new HashSet<Long>();
        this.setOfCorruptDataPages = new HashSet<Long>();
        this.mapOfBadUserAddresses = new HashMap<Long, List<Long>>();
        this.fileSize = fileSize;
        this.addressRecovery = addressRecovery;
    }

    public boolean copacetic()
    {
        return mapOfBadAddressPages.size() == 0
            && setOfBadAddressChecksums.size() == 0
            && setOfBadUserChecksums.size() == 0
            && setOfCorruptDataPages.size() == 0
            && mapOfBadUserAddresses.size() == 0;
    }

    public Pager getPager()
    {
        return pager;
    }
    
    public long getFileSize()
    {
        return fileSize;
    }
    
    public boolean isAddressRecovery()
    {
        return addressRecovery;
    }

    public Checksum getChecksum()
    {
        return checksum;
    }

    public int getBlockCount()
    {
        return blockCount;
    }
    
    public void incBlockCount()
    {
        blockCount++;
    }
    
    public void badAddressChecksum(long position)
    {
        setOfBadAddressChecksums.add(position);
    }
    
    public void badAddress(long address, long position)
    {
        long page = address / pager.getPageSize() * pager.getPageSize(); 
        Map<Long, Long> mapOfBadAddresses = mapOfBadAddressPages.get(page);
        if (mapOfBadAddresses == null)
        {
            mapOfBadAddresses = new HashMap<Long, Long>();
            mapOfBadAddressPages.put(page, mapOfBadAddresses);
        }
        mapOfBadAddresses.put(address, position);
    }

    public void corruptDataPage(long position)
    {
        setOfCorruptDataPages.add(position);
    }

    public void badUserChecksum(long position)
    {
        setOfBadUserChecksums.add(position);
    }
    
    public void badUserAddress(long position, long address)
    {
        List<Long> listOfAddresses = mapOfBadUserAddresses.get(position);
        if (listOfAddresses == null)
        {
            listOfAddresses = new ArrayList<Long>();
            mapOfBadUserAddresses.put(position, listOfAddresses);
        }
        listOfAddresses.add(address);
    }
}