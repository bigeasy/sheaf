package com.goodworkalan.pack;

public final class NullMoveRecorder
implements MoveRecorder
{
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