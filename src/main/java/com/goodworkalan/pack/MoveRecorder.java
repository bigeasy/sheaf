package com.goodworkalan.pack;

interface MoveRecorder
{
    public boolean involves(long position);

    public boolean record(Move move, boolean moved);
    
    public void clear();
}