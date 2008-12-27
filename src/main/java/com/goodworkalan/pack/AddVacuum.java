package com.goodworkalan.pack;

import java.nio.ByteBuffer;


final class AddVacuum
extends Operation
{
    private long from;
    
    private long to;
    
    private long checksum;
    
    private int offset;
    
    public AddVacuum()
    {
    }
    
    public AddVacuum(Mirror mirror, BlockPage to)
    {
        this.from = mirror.getMirrored().getRawPage().getPosition();
        this.to = to.getRawPage().getPosition();
        this.checksum = mirror.getChecksum();
        this.offset = mirror.getOffset();
    }
    
    public void vacuum(Player player)
    {
        InterimPage mirrored = player.getPager().getPage(from, new InterimPage());
        UserPage user = player.getPager().getPage(to, new UserPage());
        mirrored.vacuum(user, player.getDirtyPages(), offset, checksum);
    }

    @Override
    public void commit(Player player)
    {
        player.getVacuumSet().add(this);
    }
    
    @Override
    public int length()
    {
        return Pack.FLAG_SIZE + Pack.POSITION_SIZE * 3 + Pack.COUNT_SIZE;
    }
    
    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.ADD_VACUUM);
        bytes.putLong(from);
        bytes.putLong(to);
        bytes.putLong(checksum);
        bytes.putInt(offset);
    }
    
    @Override
    public void read(ByteBuffer bytes)
    {
        this.from = bytes.getLong();
        this.to = bytes.getLong();
        this.checksum = bytes.getLong();
        this.offset = bytes.getInt();
    }
}