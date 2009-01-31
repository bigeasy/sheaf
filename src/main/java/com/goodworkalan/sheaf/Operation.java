package com.goodworkalan.sheaf;

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

    /**
     * Return the length of the operation in the journal including the type
     * flag.
     * 
     * @return The length of this operation in the journal.
     */
    public abstract int length();

    public abstract void write(ByteBuffer bytes);

    public abstract void read(ByteBuffer bytes);
}