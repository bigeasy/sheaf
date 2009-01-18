package com.goodworkalan.pack;

/**
 * A move recorder records the moves 
 * @author alan
 */
interface MoveRecorder
{
    /**
     * Return true if the move recorder is tracking the given position.
     * 
     * @param position The position.
     * 
     * @return True if the move recorder is tracking the given position.
     */
    public boolean involves(long position);

    public boolean record(Move move, boolean moved);
    
    public void clear();
}