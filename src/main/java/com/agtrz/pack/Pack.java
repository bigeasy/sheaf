/* Copyright Alan Gutierrez 2006 */
package com.agtrz.pack;

import java.io.File;
import java.util.List;

/**
 * Management of a file as a reusable randomly accessible blocks of data.
 */
public class Pack
{
    /**
     * A constant value of the null address value of 0.
     */
    public final static long NULL_ADDRESS = 0L;

    public final static int ERROR_FREED_FREE_ADDRESS = 300;
    
    public final static int ERROR_FREED_STATIC_ADDRESS = 301;

    public final static int ERROR_READ_FREE_ADDRESS = 302;

    public final static int ERROR_FILE_NOT_FOUND = 400;
    
    public final static int ERROR_IO_WRITE = 401;

    public final static int ERROR_IO_READ = 402;

    public final static int ERROR_IO_SIZE = 403;

    public final static int ERROR_IO_TRUNCATE = 404;

    public final static int ERROR_IO_FORCE = 405;

    public final static int ERROR_IO_CLOSE = 406;

    public final static int ERROR_IO_STATIC_PAGES = 407;
    
    public final static int ERROR_SIGNATURE = 501;

    public final static int ERROR_SHUTDOWN = 502;

    public final static int ERROR_FILE_SIZE = 503;
    
    public final static int ERROR_HEADER_CORRUPT = 600;

    public final static int ERROR_BLOCK_PAGE_CORRUPT = 601;
    
    public final static int ERROR_CORRUPT = 602;

    final static long SIGNATURE = 0xAAAAAAAAAAAAAAAAL;
    
    final static int SOFT_SHUTDOWN = 0xAAAAAAAA;

    final static int HARD_SHUTDOWN = 0x55555555;
    
    final static int FLAG_SIZE = 2;

    final static int COUNT_SIZE = 4;

    final static int POSITION_SIZE = 8;

    final static int CHECKSUM_SIZE = 8;

    public final static int ADDRESS_SIZE = Long.SIZE / Byte.SIZE;

    final static int FILE_HEADER_SIZE = COUNT_SIZE * 5 + ADDRESS_SIZE * 5;

    public final static int BLOCK_PAGE_HEADER_SIZE = CHECKSUM_SIZE + COUNT_SIZE;

    final static int BLOCK_HEADER_SIZE = POSITION_SIZE + COUNT_SIZE;
    
    final static short ADD_VACUUM = 1;

    final static short VACUUM = 2;

    final static short ADD_MOVE = 3;

    final static short SHIFT_MOVE = 4;

    final static short CREATE_ADDRESS_PAGE = 5;
    
    final static short WRITE = 6;
    
    final static short FREE = 7;

    final static short NEXT_PAGE = 8;

    final static short COPY = 9;

    final static short TERMINATE = 10;
    
    final static short TEMPORARY = 11;

    final static int NEXT_PAGE_SIZE = FLAG_SIZE + ADDRESS_SIZE;

    final static int ADDRESS_PAGE_HEADER_SIZE = CHECKSUM_SIZE;

    final static int JOURNAL_PAGE_HEADER_SIZE = CHECKSUM_SIZE + COUNT_SIZE;
    
    final static int COUNT_MASK = 0xA0000000;
    
    final Pager pager;
    
    /**
     * Create a new pack from the specified pager.
     * <p>
     * One of these days, I'll have to determine if the pager class contents
     * could be within the pack.
     */
    public Pack(Pager pager)
    {
        this.pager = pager;
    }

    /**
     * Create an object that can inspect and alter the contents of this pack.
     * 
     * @return A new {@link Mutator}.
     */
    public Mutator mutate()
    {
        final PageRecorder pageRecorder = new PageRecorder();
        final MoveList listOfMoves = new MoveList(pageRecorder, pager.getMoveList());
        return listOfMoves.mutate(new GuardedReturnable<Mutator>()
        {
            public Mutator run(List<MoveLatch> listOfMoveLatches)
            {
                MoveNodeRecorder moveNodeRecorder = new MoveNodeRecorder();
                DirtyPageMap dirtyPages = new DirtyPageMap(pager, 16);
                Journal journal = new Journal(pager, moveNodeRecorder, pageRecorder, dirtyPages);
                return new Mutator(pager, listOfMoves, moveNodeRecorder, pageRecorder, journal, dirtyPages);
            }
        });
    }

    /**
     * Soft close of the pack will wait until all mutators commit or rollback
     * and then compact the pack before closing the file.
     */
    public void close()
    {
        pager.close();
    }
    
    public File getFile()
    {
        return pager.getFile();
    }

    public void copacetic()
    {
    }
}

/* vim: set et sw=4 ts=4 ai tw=80 nowrap: */
