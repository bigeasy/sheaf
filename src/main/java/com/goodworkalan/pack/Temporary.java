package com.goodworkalan.pack;

import java.nio.ByteBuffer;


final class Temporary
extends Operation
{
    private long address;
    
    private long temporary;
    
    public Temporary()
    {
    }
    
    public Temporary(long address, long temporary)
    {
        this.address = address;
        this.temporary = temporary;
    }
    
    @Override
    public void commit(Player player)
    {
        player.getPager().setTemporary(address, temporary, player.getDirtyPages());
    }
    
    public void rollback(Pager pager)
    {
        pager.rollbackTemporary(address, temporary);
    }

    @Override
    public int length()
    {
        return Pack.FLAG_SIZE + Pack.ADDRESS_SIZE * 2;
    }
    
    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.TEMPORARY);
        bytes.putLong(address);
        bytes.putLong(temporary);
    }
    
    @Override
    public void read(ByteBuffer bytes)
    {
        address = bytes.getLong();
        temporary = bytes.getLong();
    }
}