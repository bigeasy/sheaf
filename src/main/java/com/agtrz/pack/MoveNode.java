package com.agtrz.pack;

final class MoveNode
{
    private final Move move;
    
    private MoveNode next;
    
    public MoveNode()
    {
        this.move = null;
    }

    public MoveNode(Move move)
    {
        this.move = move;
    }
    
    public Move getMove()
    {
        return move;
    }
    
    public MoveNode extend(Move move)
    {
        assert next == null;
        
        return next = new MoveNode(move);
    }

    public MoveNode getNext()
    {
        return next;
    }
    
    public MoveNode getLast()
    {
        MoveNode iterator = this;
        while (iterator.next == null)
        {
            iterator = iterator.next;
        }
        return iterator;
    }
}