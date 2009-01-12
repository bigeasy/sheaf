package com.goodworkalan.pack;

import java.nio.ByteBuffer;


final class Free
extends Operation
{
    private long address;
    
    private long position;
    
    public Free()
    {
    }

    public Free(long address, long position)
    {
        this.address = address;
        this.position = position;
    }

    @Override
    public void commit(Player player)
    {
        // FIXME Someone else can allocate the address and even the block
        // now that it is free and the replay ruins it. 
        Pager pager = player.getPager();
        pager.getAddressLocker().lock(player.getAddressSet(), address);
        // FIXME Same problem with addresses as with temporary headers,
        // someone can reuse when we're scheduled to release.
        pager.freeTemporary(address, player.getDirtyPages());
        AddressPage addresses = pager.getPage(address, AddressPage.class, new AddressPage());
        addresses.free(address, player.getDirtyPages());
        UserPage user = pager.getPage(player.adjust(position), UserPage.class, new UserPage());
        user.waitOnMirrored();
        pager.getFreePageBySize().reserve(user.getRawPage().getPosition());
        user.free(address, player.getDirtyPages());
        pager.getFreePageBySize().release(user.getRawPage().getPosition());
        pager.returnUserPage(user);
    }

    @Override
    public int length()
    {
        return Pack.FLAG_SIZE + Pack.ADDRESS_SIZE + Pack.POSITION_SIZE;
    }

    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.FREE);
        bytes.putLong(address);
        bytes.putLong(position);
    }

    @Override
    public void read(ByteBuffer bytes)
    {
        address = bytes.getLong();
        position = bytes.getLong();
    }
}