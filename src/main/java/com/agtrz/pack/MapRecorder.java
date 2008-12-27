package com.agtrz.pack;

import java.util.TreeMap;

final class MapRecorder
extends TreeMap<Long, Movable>
implements MoveRecorder
{
    private static final long serialVersionUID = 1L;

    public boolean involves(long position)
    {
        return containsKey(position) || containsKey(-position);
    }
    
    public boolean record(Move move, boolean moved)
    {
        if (containsKey(move.getFrom()))
        {
            put(move.getTo(), remove(move.getFrom()));
            moved = true;
        }
        if (containsKey(-move.getFrom()))
        {
            put(move.getFrom(), remove(-move.getFrom()));
        }
        return moved;
    }
}