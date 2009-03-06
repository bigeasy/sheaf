package com.goodworkalan.sheaf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;

public abstract class BasicWritable extends AbstractRegion implements Writable
{
    protected final Cleanable cleanable;
    
    public BasicWritable(long position, ByteBuffer byteBuffer, Lock lock, Cleanable cleanable)
    {
        super(position, byteBuffer, lock);
        this.cleanable = cleanable;
    }

    @Override
    protected Dirtyable getDirtyable()
    {
        return cleanable;
    }

    public void write(FileChannel fileChannel, int offset) throws IOException
    {
        ByteBuffer bytes = getByteBuffer();
        bytes.clear();
        fileChannel.write(bytes, offset + getPosition());
        cleanable.clean();
    }
}
