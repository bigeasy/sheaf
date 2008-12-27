package com.agtrz.pack;

final class Mirror
{
    private final BlockPage mirrored;

    private final int offset;
    
    private final long checksum;
    
    public Mirror(BlockPage mirrored, int offset, long checksum)
    {
        this.mirrored = mirrored;
        this.offset = offset;
        this.checksum = checksum;
    }
    
    public BlockPage getMirrored()
    {
        return mirrored;
    }
    
    public int getOffset()
    {
        return offset;
    }
    
    public long getChecksum()
    {
        return checksum;
    }
}