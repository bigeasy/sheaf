package com.goodworkalan.sheaf;

/**
 * The move recorder used the the move latch list kept by the pager, since the
 * pager does not actually track moves.
 * 
 * @author Alan Gutierrez
 */
final class NullMoveRecorder
implements MoveRecorder
{
    /**
     * Always returns false since this move recorder tracks no pages.
     * 
      * @param position
     *            The position.
     * 
     * @return False since the move recorder tracks no pages.
     */
    public boolean involves(long position)
    {
        return false;
    }

    public boolean record(Move move, boolean moved)
    {
        return moved;
    }
    
    public void clear()
    {
    }
}