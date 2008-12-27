package com.goodworkalan.pack;

import java.nio.ByteBuffer;


final class Vacuum
extends Operation
{
    private long newJournalPosition;
    
    public Vacuum()
    {
    }
    
    public Vacuum(long position)
    {
        this.newJournalPosition = position;
    }

    @Override
    public void commit(Player player)
    {
        for (AddVacuum addVacuum: player.getVacuumSet())
        {
            addVacuum.vacuum(player);
        }
        // FIXME Prettify.
        ByteBuffer bytes = player.getJournalHeader().getByteBuffer();
        bytes.clear();
        long oldJournalPosition = bytes.getLong();
        if (oldJournalPosition != newJournalPosition)
        {
            bytes.clear();
            bytes.putLong(newJournalPosition);
            player.getDirtyPages().flush(player.getJournalHeader());
        }
    }

    @Override
    public int length()
    {
        return Pack.FLAG_SIZE;
    }
    
    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.VACUUM);
        bytes.putLong(newJournalPosition);
    }
    
    @Override
    public void read(ByteBuffer bytes)
    {
        this.newJournalPosition = bytes.getLong();
    }
}