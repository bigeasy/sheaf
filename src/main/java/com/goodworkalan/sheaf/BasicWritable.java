package com.goodworkalan.sheaf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;

// TODO Document.
public abstract class BasicWritable extends AbstractRegion implements Writable
{
    // TODO Document.
    protected final Cleanable cleanable;
    
    // TODO Document.
    public BasicWritable(long position, ByteBuffer byteBuffer, Lock lock, Cleanable cleanable)
    {
        super(position, byteBuffer, lock);
        this.cleanable = cleanable;
    }

    // TODO Document.
    @Override
    protected Dirtyable getDirtyable()
    {
        return cleanable;
    }

    // TODO Document.
    public void write(FileChannel fileChannel, int offset) throws IOException
    {
        ByteBuffer bytes = getByteBuffer();
        bytes.clear();
        fileChannel.write(bytes, offset + getPosition());
        cleanable.clean();
    }
}
