package com.goodworkalan.sheaf;

final class Offsets
{
    private final int pageSize;
    
    private final int internalJournalCount;
    
    private final int staticPageMapSize;
    
    public Offsets(int pageSize, int internalJournalCount, int staticPageMapSize)
    {
        this.pageSize = pageSize;
        this.internalJournalCount = internalJournalCount;
        this.staticPageMapSize = staticPageMapSize;
    }
    
    public int getPageSize()
    {
        return pageSize;
    }
    
    public int getInternalJournalCount()
    {
        return internalJournalCount;
    }
    private long getEndOfHeader()
    {
        return Pack.FILE_HEADER_SIZE + staticPageMapSize + internalJournalCount * Pack.POSITION_SIZE; 
    }
    
    public long getFirstAddressPageStart()
    {
        long endOfHeader = getEndOfHeader();
        long remaining = pageSize - endOfHeader % pageSize;
        if (remaining < Pack.ADDRESS_PAGE_HEADER_SIZE + Pack.ADDRESS_SIZE)
        {
            return  (endOfHeader + pageSize - 1) / pageSize * pageSize;
        }
        return endOfHeader;
    }
    
    public long getFirstAddressPage()
    {
        return getFirstAddressPageStart() / pageSize * pageSize;
    }

    public long getDataBoundary()
    {
        return (getFirstAddressPageStart() + pageSize - 1) / pageSize * pageSize;
    }
}