package com.agtrz.pack;

import java.nio.ByteBuffer;


final class Write
extends Operation
{
    private long address;
    
    private long from;
    
    public Write()
    {
    }
    
    public Write(long address, long from)
    {
        this.address = address;
        this.from = from;
    }
    
    @Override
    public void commit(Player player)
    {
        Pager pager = player.getPager();
        InterimPage interim = pager.getPage(from, new InterimPage());
        interim.write(address, player.getDirtyPages());
    }
    
    @Override
    public int length()
    {
        return Pack.FLAG_SIZE + Pack.ADDRESS_SIZE + Pack.POSITION_SIZE;
    }
    
    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.WRITE);
        bytes.putLong(address);
        bytes.putLong(from);
    }
    
    @Override
    public void read(ByteBuffer bytes)
    {
        address = bytes.getLong();
        from = bytes.getLong();
    }
}