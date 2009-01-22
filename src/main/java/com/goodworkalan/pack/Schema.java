package com.goodworkalan.pack;

import java.net.URI;

/**
 * Structure representing the properties used to create a <code>Pack</code>
 * file. Given a <code>Schema</code> object, one should be able to create a new
 * <code>Pack</code>, but I haven't gotten there yet. And, uh, why is that
 * desirable? Maybe to know some things about the <code>Pack</code> is good,
 * like page size and alignment, but the creational pattern, it is not like
 * cloning is desirable or necessary. Isnt' all this kept in the creator anyway?
 * An interface too far.
 * 
 * TODO Maybe rename structure?
 * 
 * @author Alan Gutierrez
 */
public interface Schema
{
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

//    public int getInternalJournalCount();

    /**
     * Return the disk object used to read and write to the file channel. The
     * disk is an class that can be overridden to generate I/O errors during
     * testing.
     * 
     * @return The disk.
     */
    public Disk getDisk();

    /**
     * Return an address mapped to a URI given during the creation of the
     * <code>Pack</code>.
     * 
     * TODO This should not be part of the schema, since the address might be
     * different for different instances of <code>Pack</code>. Does this
     * schema object go away? Shouldn't this be a map of URIs to sizes?
     * 
     * @param uri
     *            The URI of a static address.
     * @return The address mapped to the given uri.
     */
    public long getStaticPageAddress(URI uri);
}