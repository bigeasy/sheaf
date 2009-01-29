package com.goodworkalan.pack;


class NullJournalWriter
extends JournalWriter
{
    public NullJournalWriter(Pager pager, MoveNodeRecorder moveNodeRecorder, PageRecorder pageRecorder, DirtyPageSet dirtyPages)
    {
        super(pager, moveNodeRecorder, pageRecorder, null, null, dirtyPages);
    }

    public boolean write(Operation operation)
    {
        return false;
    }

    /**
     * Create a new journal to record mutations. The dirty page map will
     * record the creation of wilderness pages.
     * <p>
     * The move node is necessary to create a movable position that will
     * track the position of journal head. This move node is the same move
     * node that is at the head of the mutator.
     * <p>
     * We will use the page recorder to record which pages we're using as
     * journal pages.
     * 
     * @param pager
     *            The pager of the mutator for the journal.
     * @param pageRecorder
     *            Records the allocation of new journal pages.
     * @param moveNode
     *            Needed to create a movable position that will reference
     *            the first journal page.
     * @param dirtyPages
     *            A dirty page map where page writes are cached before
     *            being written to disk.
     */
    public JournalWriter extend()
    {
        JournalPage journal = pager.newInterimPage(new JournalPage(), dirtyPages);
        Movable start = new Movable(moveNodeRecorder.getMoveNode(), journal.getJournalPosition(), 0);
        pageRecorder.getJournalPages().add(journal.getRawPage().getPosition());
        return new JournalWriter(pager, moveNodeRecorder, pageRecorder, journal, start, dirtyPages);
    }
}