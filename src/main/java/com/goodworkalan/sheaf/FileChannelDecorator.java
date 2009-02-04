package com.goodworkalan.sheaf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class FileChannelDecorator extends FileChannel
{
    /** The actual file channel. */
    private final FileChannel fileChannel;
    
    /**
     * Create a file channel that delegates to the given file channel.
     * 
     * @param fileChannel The delegate file channel.
     */
    public FileChannelDecorator(FileChannel fileChannel)
    {
        this.fileChannel = fileChannel;
    }

    @Override
    public void force(boolean metaData) throws IOException
    {
        fileChannel.force(metaData);
    }

    @Override
    public FileLock lock(long position, long size, boolean shared)
            throws IOException
    {
        return fileChannel.lock(position, size, shared);
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size)
            throws IOException
    {
        return fileChannel.map(mode, position, size);
    }

    @Override
    public long position() throws IOException
    {
        return fileChannel.position();
    }

    @Override
    public FileChannel position(long newPosition) throws IOException
    {
        return fileChannel.position(newPosition);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException
    {
        return fileChannel.read(dst);
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException
    {
        return fileChannel.read(dst, position);
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length)
            throws IOException
    {
        return fileChannel.read(dsts, offset, length);
    }

    @Override
    public long size() throws IOException
    {
        return fileChannel.size();
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count)
            throws IOException
    {
        return fileChannel.transferFrom(src, position, count);
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target)
            throws IOException
    {
        return fileChannel.transferTo(position, count, target);
    }

    @Override
    public FileChannel truncate(long size) throws IOException
    {
        return fileChannel.truncate(size);
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared)
            throws IOException
    {
        return fileChannel.tryLock();
    }

    @Override
    public int write(ByteBuffer src) throws IOException
    {
        return fileChannel.write(src);
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException
    {
        return fileChannel.write(src, position);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length)
            throws IOException
    {
        return fileChannel.write(srcs, offset, length);
    }

    @Override
    protected void implCloseChannel() throws IOException
    {
        fileChannel.close();
    }
}
