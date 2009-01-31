package com.goodworkalan.sheaf;

import java.nio.ByteBuffer;

final class Terminate
extends Operation
{
    @Override
    public boolean terminate()
    {
        return true;
    }

    @Override
    public int length()
    {
        return Pack.FLAG_SIZE;
    }

    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.TERMINATE);
    }
    
    @Override
    public void read(ByteBuffer bytes)
    {
    }
}