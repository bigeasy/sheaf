package com.goodworkalan.sheaf;

/**
 * A structure to contain the results of mirroring a user block page to
 * an interim block page.
 * 
 * @author Alan Gutierrez
 */
final class Mirror
{
    /** The interim block page used to store the mirror. */
    private final BlockPage mirrored;

    /** The offset of the first mirrored block in the user page. */
    private final int offset;
    
    /** A checksum of the user page after the mirror is copied. */
    private final long checksum;
    
    /**
     * Create a mirror structure to record the results of the mirroring
     * of a user block page to an interim block page.
     * 
     * @param mirrored
     * @param offset
     * @param checksum
     */
    public Mirror(BlockPage mirrored, int offset, long checksum)
    {
        this.mirrored = mirrored;
        this.offset = offset;
        this.checksum = checksum;
    }
    
    /**
     * Return the interim block page used to store the mirror.
     * 
     * @return The interim block page used to store the mirror.
     */
    public BlockPage getMirrored()
    {
        return mirrored;
    }
    
    
    /**
     * Return the offset of the first mirrored block in the user page. When
     * mirroring for vacuum, the blocks at the start of the user block page
     * that are not separated by free blocks are not copied into the mirror.
     * 
     * @return The interim block page used to store the mirror.
     */
    public int getOffset()
    {
        return offset;
    }

    /**
     * Return a checksum of the user page after the mirror is copied.
     * 
     * @return A checksum of the user page after the mirror is copied.
     */
    public long getChecksum()
    {
        return checksum;
    }
}