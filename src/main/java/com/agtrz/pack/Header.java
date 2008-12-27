package com.agtrz.pack;

import java.nio.ByteBuffer;

final class Header extends Regional
{
    private final ByteBuffer bytes;
    
    public Header(ByteBuffer bytes)
    {
        super(0L);
        this.bytes = bytes;
    }
    
    public ByteBuffer getByteBuffer()
    {
        return bytes;
    }
    
    public long getStaticPagesStart()
    {
        return Pack.FILE_HEADER_SIZE + getInternalJournalCount() * Pack.POSITION_SIZE;
    }

    // FIXME Make this a checksum.
    public long getSignature()
    {
        return bytes.getLong(0);
    }
    
    public void setSignature(long signature)
    {
        bytes.putLong(0, signature);
        invalidate(0, Pack.CHECKSUM_SIZE);
    }
    
    public int getShutdown()
    {
        return bytes.getInt(Pack.CHECKSUM_SIZE);
    }
    
    public void setShutdown(int shutdown)
    {
        bytes.putInt(Pack.CHECKSUM_SIZE, shutdown);
        invalidate(Pack.CHECKSUM_SIZE, Pack.COUNT_SIZE);
    }
    
    public int getPageSize()
    {
        return bytes.getInt(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE);
    }
    
    public void setPageSize(int pageSize)
    {
        bytes.putInt(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE, pageSize);
        invalidate(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE, Pack.COUNT_SIZE);
    }
    
    public int getAlignment()
    {
        return bytes.getInt(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 2);
    }
    
    public void setAlignment(int alignment)
    {
        bytes.putInt(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 2, alignment);
        invalidate(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 2, Pack.COUNT_SIZE);
    }
    
    public int getInternalJournalCount()
    {
        return bytes.getInt(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 3);
    }
    
    public void setInternalJournalCount(int internalJournalCount)
    {
        bytes.putInt(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 3, internalJournalCount);
        invalidate(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 3, Pack.COUNT_SIZE);
    }
    
    public int getStaticPageSize()
    {
        return bytes.getInt(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 4);
    }
    
    public void setStaticPageSize(int staticPageSize)
    {
        bytes.putInt(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 4, staticPageSize);
        invalidate(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 4, Pack.COUNT_SIZE);
    }
    
    public long getFirstAddressPageStart()
    {
        return bytes.getLong(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 5);
    }
    
    public void setFirstAddressPageStart(long firstAddressPageStart)
    {
        bytes.putLong(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 5, firstAddressPageStart);
        invalidate(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 5, Pack.ADDRESS_SIZE);
    }

    public long getDataBoundary()
    {
        return bytes.getLong(Pack.CHECKSUM_SIZE * 2 + Pack.COUNT_SIZE * 5);
    }
    
    public void setDataBoundary(long dataBoundary)
    {
        bytes.putLong(Pack.CHECKSUM_SIZE * 2 + Pack.COUNT_SIZE * 5, dataBoundary);
        invalidate(Pack.CHECKSUM_SIZE * 2 + Pack.COUNT_SIZE * 5, Pack.ADDRESS_SIZE);
    }
    
    public long getOpenBoundary()
    {
        return bytes.getLong(Pack.CHECKSUM_SIZE * 3 + Pack.COUNT_SIZE * 5);
    }
    
    public void setOpenBoundary(long openBoundary)
    {
        bytes.putLong(Pack.CHECKSUM_SIZE * 3 + Pack.COUNT_SIZE * 5, openBoundary);
        invalidate(Pack.CHECKSUM_SIZE * 3 + Pack.COUNT_SIZE * 5, Pack.ADDRESS_SIZE);
    }
    
    public long getTemporaries()
    {
        return bytes.getLong(Pack.CHECKSUM_SIZE * 4 + Pack.COUNT_SIZE * 5);
    }
    
    public void setTemporaries(long temporaries)
    {
        bytes.putLong(Pack.CHECKSUM_SIZE * 4 + Pack.COUNT_SIZE * 5, temporaries);
        invalidate(Pack.CHECKSUM_SIZE * 4 + Pack.COUNT_SIZE * 5, Pack.ADDRESS_SIZE);
    }
}