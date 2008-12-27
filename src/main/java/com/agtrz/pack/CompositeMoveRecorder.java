package com.agtrz.pack;

import java.util.ArrayList;
import java.util.List;

class CompositeMoveRecorder
implements MoveRecorder
{
    private final List<MoveRecorder> listOfMoveRecorders;
    
    public CompositeMoveRecorder()
    {
        this.listOfMoveRecorders = new ArrayList<MoveRecorder>();
    }
    
    public void add(MoveRecorder recorder)
    {
        listOfMoveRecorders.add(recorder);
    }
    
    public boolean involves(long position)
    {
        for (MoveRecorder recorder: listOfMoveRecorders)
        {
            if (recorder.involves(position))
            {
                return true;
            }
        }
        return false;
    }
    
    public boolean record(Move move, boolean moved)
    {
        for (MoveRecorder recorder: listOfMoveRecorders)
        {
            moved = recorder.record(move, moved);
        }
        return moved;
    }
    
    public void clear()
    {
        for (MoveRecorder recorder: listOfMoveRecorders)
        {
            recorder.clear();
        }
    }
}