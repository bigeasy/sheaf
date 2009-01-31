package com.goodworkalan.sheaf;

final class Move
{
    private final long from;
    
    private final long to;
    
    public Move(long from, long to)
    {
        if (from == to || from == 0 || to == 0)
        {
            throw new IllegalArgumentException();
        }

        this.from = from;
        this.to = to;
    }
    
    public long getFrom()
    {
        return from;
    }
    
    public long getTo()
    {
        return to;
    }
}