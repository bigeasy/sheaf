package com.goodworkalan.pack;

import java.nio.ByteBuffer;


abstract class Operation
{
    public void commit(Player player)
    {
    }

    public JournalPage getJournalPage(Player player, JournalPage journalPage)
    {
        return journalPage;
    }

    public boolean terminate()
    {
        return false;
    }

    public abstract int length();

    public abstract void write(ByteBuffer bytes);

    public abstract void read(ByteBuffer bytes);
}