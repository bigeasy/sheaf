package com.goodworkalan.pack;

import java.nio.ByteBuffer;


final class NextOperation
extends Operation
{
    private long position;

    public NextOperation()
    {
    }

    public NextOperation(long position)
    {
        this.position = position;
    }
    
    @Override
    public JournalPage getJournalPage(Player player, JournalPage journalPage)
    {
        journalPage = player.getPager().getPage(player.adjust(position), JournalPage.class, new JournalPage());
        journalPage.seek(player.adjust(position));
        return journalPage;
    }

    public int length()
    {
        return Pack.FLAG_SIZE + Pack.ADDRESS_SIZE;
    }

    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.NEXT_PAGE);
        bytes.putLong(position);
    }

    public void read(ByteBuffer bytes)
    {
        position = bytes.getLong();
    }
}