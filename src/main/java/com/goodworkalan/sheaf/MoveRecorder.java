package com.goodworkalan.sheaf;

/**
 * Tracks the movement of pages performing tasks like updating sets of page
 * positions or maintaining linked lists of moves for <code>Movable</code> page
 * references.
 * 
 * @author Alan Gutierrez
 */
interface MoveRecorder
{
    /**
     * Return true if the move recorder is tracking the given position.
     * 
     * @param position
     *            The position.
     * 
     * @return True if the move recorder is tracking the given position.
     */
    public boolean involves(long position);

    /**
     * Record a move returning the given value for moved if this move recorder
     * was not affected by move or true if it was effected by the move.
     * <p>
     * TODO The return value is referenced in the journal move recorder.
     * 
     * @param move
     *            The move to record.
     * @param moved
     *            The value to return if this move recorder was not affected by
     *            the move.
     * @return The given value of moved or true if this move recorder was
     *         affected by the move.
     */
    public boolean record(Move move, boolean moved);

    /**
     * Reset the move node recorder for subsequent use of a mutator after commit
     * or rollback.
     */
    public void clear();
}