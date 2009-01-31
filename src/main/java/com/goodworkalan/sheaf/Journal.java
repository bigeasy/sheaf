package com.goodworkalan.sheaf;

class Journal
{
    private JournalWriter writer;
    
    public Journal(Pager pager, MoveNodeRecorder moveNodeRecorder, PageRecorder pageRecorder, DirtyPageSet dirtyPages)
    {
        writer = new NullJournalWriter(pager, moveNodeRecorder, pageRecorder, dirtyPages);
    }
    
    public Movable getJournalStart()
    {
        return writer.getJournalStart();
    }
    
    public long getJournalPosition()
    {
        return writer.getJournalPosition();
    }

    public void write(Operation operation)
    {
        while (!writer.write(operation))
        {
            writer = writer.extend();
        }
    }
    
    public void reset()
    {
        writer = writer.reset();
    }
}