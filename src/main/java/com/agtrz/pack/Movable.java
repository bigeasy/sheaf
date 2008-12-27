package com.agtrz.pack;


class Movable
{
    private final MoveNode moveNode;
    
    private final long position;
    
    private final int skip;
    
    public Movable(MoveNode moveNode, long position, int skip)
    {
        this.moveNode = moveNode;
        this.position = position;
        this.skip = skip;
    }
    
    public long getPosition(Pager pager)
    {
        return pager.adjust(moveNode, position, skip);
    }
}