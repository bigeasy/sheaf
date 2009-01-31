/* Copyright Alan Gutierrez 2006 */
package com.goodworkalan.sheaf;

import java.io.File;
import java.net.URI;
import java.util.Map;

/**
 * Management of a file as a reusable randomly accessible blocks of data.
 */
public interface Pack
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
    
    /**
     * Get the size of all underlying pages managed by this pager.
     * 
     * @return The page size.
     */
    public int getPageSize();

    /**
     * Return the alignment to which all block allocations are rounded.
     * 
     * @return The block alignment.
     */
    public int getAlignment();

    /**
     * Soft close of the pack will wait until all mutators commit or rollback
     * and then compact the pack before closing the file.
     */
    public void close();
    
    public Mutator mutate();
    
    public File getFile();

    public void copacetic();

    /**
     * Return a map of named pages that maps a URI to the address of a static
     * page. Static pages are defined using the
     * {@link Creator#addStaticPage(URI, int)} method. They can be used to
     * specify blocks that contain housekeeping information in application
     * programs.
     * 
     * @return The map of named static pages.
     */
    public Map<URI, Long> getStaticPages();
}

/* vim: set et sw=4 ts=4 ai tw=80 nowrap: */
