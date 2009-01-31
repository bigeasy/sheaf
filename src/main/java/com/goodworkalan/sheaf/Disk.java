package com.goodworkalan.sheaf;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Wrapper around calls to <code>FileChannel</code> methods that allows
 * for simulating IO failures in testing. The <code>Disk</code> class is
 * stateless. Methods take a <code>FileChannel</code> as a parameter. The
 * default implementation forwards the calls directly to the
 * <code>FileChannel</code>.
 * <p>
 * This class is generally useful to testing. Users are encouraged to
 * subclass <code>Disk</code> to throw <code>IOException</code>
 * exceptions for their own unit tests. For example, subclass of disk can
 * inspect writes for a particular file position and fail after a certain
 * amount of writes.
 * <p>
 * The <code>Disk</code> class is set using the
 * {@link Creator#setDisk Pack.Creator.setDisk} or
 * {@link Opener#setDisk Pack.Opener.setDisk} methods. Because it is
 * stateless it can be used for multiple <code>Pack</code> instances.
 * However the only intended use case for a subclass of <code>Disk</code>
 * to generate I/O failures. These subclasses are not expected to be
 * stateless.
 */
public class Disk
{
    /**
     * Create an instance of <code>Disk</code>.
     */
    public Disk()
    {
    }

    /**
     * Open a file and create a <code>FileChannel</code>.
     * 
     * @param file
     *            The file to open.
     * @return A <code>FileChannel</code> open on the specified file.
     * @throws FileNotFoundException
     *             If the file exists but is a directory rather than a
     *             regular file, or cannot be opened or created for any
     *             other reason.
     */
    public FileChannel open(File file) throws FileNotFoundException
    {
        return new RandomAccessFile(file, "rw").getChannel();
    }

    /**
     * Writes a sequence of bytes to the file channel from the given buffer,
     * starting at the given file position.
     * 
     * @param fileChannel
     *            The file channel to which to write.
     * @param src
     *            The buffer from which bytes are transferred.
     * @param position
     *            The file position at which the transfer is to begin, must
     *            be non-negative.
     * @return The number of bytes written, possibly zero.
     * @throws IOException
     *             If an I/O error occurs.
     */
    public int write(FileChannel fileChannel, ByteBuffer src, long position) throws IOException
    {
        return fileChannel.write(src, position);
    }

    /**
     * Reads a sequence of bytes from the file channel into the given
     * buffer, starting at the given file position.
     * 
     * @param fileChannel
     *            The file channel to which to write.
     * @param dst
     *            The buffer into which bytes are transferred.
     * @param position
     *            The file position at which the transfer is to begin, must
     *            be non-negative.
     * @return The number of bytes read, possibly zero, or -1 if the given
     *         position is greater than or equal to the file's current size.
     * @throws IOException
     *             If an I/O error occurs.
     */
    public int read(FileChannel fileChannel, ByteBuffer dst, long position) throws IOException
    {
        return fileChannel.read(dst, position);
    }

    /**
     * Returns the current size of the file channel's file.
     * 
     * @param fileChannel
     *            The file channel to size.
     * @return The current size of this channel's file, measured in bytes.
     * @throws IOException
     *             If an I/O error occurs.
     */
    public long size(FileChannel fileChannel) throws IOException
    {
        return fileChannel.size();
    }

    /**
     * Truncates the file channel to the specified size.
     * 
     * @param fileChannel
     *            The file channel to truncate.
     * @param size
     *            The new size of the file channel, measured in bytes.
     * @return The file channel provided.
     * @throws IOException
     *             If some other I/O error occurs
     */
    public FileChannel truncate(FileChannel fileChannel, long size) throws IOException
    {
        return fileChannel.truncate(size);
    }

    /**
     * Forces any updates to the file channel's file to be written to the
     * storage device that contains it.
     * 
     * @param fileChannel
     *            The file channel to flush.
     * @throws IOException
     *             If an I/O error occurs.
     */
    public void force(FileChannel fileChannel) throws IOException
    {
        fileChannel.force(true);
    }

    /**
     * Closes the file channel.
     * 
     * @param fileChannel
     *            The file channel to close.
     * @throws IOException
     *             If an I/O error occurs.
     */
    public void close(FileChannel fileChannel) throws IOException
    {
        fileChannel.close();
    }
}