package com.goodworkalan.pack;

final class BySizeTableRecorder
implements MoveRecorder
{
    private final BySizeTable bySizeTable;
    
    public BySizeTableRecorder(BySizeTable bySizeTable)
    {
        this.bySizeTable = bySizeTable;
    }
    
    public boolean involves(long position)
    {
        return bySizeTable.contains(position);
    }
    
    public boolean record(Move move, boolean moved)
    {
        int size = 0;
        if ((size = bySizeTable.remove(move.getFrom())) != 0)
        {
            bySizeTable.add(move.getTo(), size);
        }
        return false;
    }
    
    public void clear()
    {
        bySizeTable.clear();
    }
}