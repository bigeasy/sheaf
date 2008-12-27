package com.goodworkalan.pack;

final class JournalRecorder
implements MoveRecorder
{
    private final Journal journal;
    
    public JournalRecorder(Journal journal)
    {
        this.journal = journal;
    }

    public boolean involves(long position)
    {
        return false;
    }
    
    public boolean record(Move move, boolean moved)
    {
        if (moved)
        {
            journal.write(new ShiftMove());
        }

        return moved;
    }
    
    public void clear()
    {
        journal.reset();
    }
}