package com.goodworkalan.sheaf;


class JournalWriter
{
    protected final JournalPage journal;

    protected final Pager pager;
    
    protected final DirtyPageSet dirtyPages;
    
    protected final PageRecorder pageRecorder;
    
    protected final Movable start;
    
    protected final MoveNodeRecorder moveNodeRecorder;
    
    public JournalWriter(Pager pager, MoveNodeRecorder moveNodeRecorder, PageRecorder pageRecorder, JournalPage journal, Movable start, DirtyPageSet dirtyPages)
    {
        this.pager = pager;
        this.pageRecorder = pageRecorder;
        this.journal = journal;
        this.start = start;
        this.dirtyPages = dirtyPages;
        this.moveNodeRecorder = moveNodeRecorder;
    }
    
    public Movable getJournalStart()
    {
        return start;
    }

    public long getJournalPosition()
    {
        return journal.getJournalPosition();
    }
    
    public boolean write(Operation operation)
    {
        return journal.write(operation, Pack.NEXT_PAGE_SIZE, dirtyPages);
    }
    
    public JournalWriter extend()
    {
        JournalPage nextJournal = pager.newInterimPage(new JournalPage(), dirtyPages);
        journal.write(new NextOperation(nextJournal.getJournalPosition()), 0, dirtyPages);
        pageRecorder.getJournalPages().add(journal.getRawPage().getPosition());
        return new JournalWriter(pager, moveNodeRecorder, pageRecorder, nextJournal, start, dirtyPages);
    }
    
    public JournalWriter reset()
    {
        return new NullJournalWriter(pager, moveNodeRecorder, pageRecorder, dirtyPages);
    }
}