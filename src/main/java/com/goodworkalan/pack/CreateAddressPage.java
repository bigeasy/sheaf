package com.goodworkalan.pack;

import java.nio.ByteBuffer;


final class CreateAddressPage
extends Operation
{
    private long position;
    
    public CreateAddressPage()
    {            
    }
    
    public CreateAddressPage(long position)
    {
        this.position = position;
    }
    
    @Override
    public void commit(Player player)
    {
        player.getPager().setPage(position, AddressPage.class, new AddressPage(), player.getDirtyPages(), true);
    }
    
    @Override
    public int length()
    {
        return Pack.FLAG_SIZE + Pack.ADDRESS_SIZE;
    }
    
    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.CREATE_ADDRESS_PAGE);
        bytes.putLong(position);
    }
    
    @Override
    public void read(ByteBuffer bytes)
    {
        position = bytes.getLong(); 
    }
}