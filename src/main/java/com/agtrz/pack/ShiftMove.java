package com.agtrz.pack;

import java.nio.ByteBuffer;


final class ShiftMove extends Operation
{
    @Override
    public void commit(Player player)
    {
        player.getMoveList().removeFirst();
    }

    @Override
    public int length()
    {
        return Pack.FLAG_SIZE;
    }
    
    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.SHIFT_MOVE);
    }
    
    @Override
    public void read(ByteBuffer bytes)
    {
    }
}