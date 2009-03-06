package com.goodworkalan.sheaf;

import java.io.IOException;
import java.nio.channels.FileChannel;

// TODO Document.
public interface Writable extends Region
{
    /**
     * Write the dirty regions to the given file channel using the given disk at
     * the position of this dirty region map offset by the given offset.
     * 
     * @param fileChannel
     *            The file channel to write to.
     * @param offset
     *            An offset to add to the dirty region map file position.
     * @throws IOException
     *             If an I/O error occurs.
     */
    public void write(FileChannel fileChannel, int offset) throws IOException;
}