package com.goodworkalan.pack;

import java.util.zip.Checksum;


/**
 * The <code>Page</code> interface is a specialized application of a
 * {@link RawPage}. It is the strategy participant in the strategy
 * design pattern. The raw page contains only the basic position and byte
 * buffer page attributes. A <code>Page</code> implementation interprets
 * the raw page as a specific page type.
 * <h4>Frequent Instanciation and Two-Step Initialization</h4>
 * <p>
 * When we request a page from the the pager, we provide and implementation
 * of <code>Page</code>. If the page exists in the pager, the pager will
 * ensure that the <code>Page</code> is assignable to the class of the
 * specified <code>Page</code>. If the page does not exist in the pager,
 * a new <code>RawPage</code> is created and the specified
 * <code>Page</code> is associated with the <code>RawPage</code>.
 * <p>
 * Thus, we create a lot of <code>Page</code> instances that are not
 * actually initialized. They are used as flags when requesting a page from
 * the <code>Pager</code>. The <code>Pager</code> will use them as
 * actual <code>Page</code> implementations only if page is not available
 * in the <code>Pager</code> or if the associated <code>Page</code>
 * implementation of the page in the <code>Pager</code> is a superclass of
 * the desired <code>Page</code> implementation.
 * <p>
 * All <code>Page</code> implementations perform minimal initialization at
 * construction. They are actually initialized by a call to either {@link
 * #create create} or {@link #load load}.
 * <h4>Circular RawPage and Page Reference</h4>
 * <p>
 * The circular reference between <code>Page</code> and
 * <code>RawPage</code> is necessary. <code>RawPage</code> is used as
 * the mutex for changes to the underlying byte buffer as well as to the
 * data members of the <code>Page</code> itself. All the <code>Page</code>
 * implementations use the associated <code>RawPage</code> as the lock
 * object in synchronized blocks as opposed to using synchronized methods on
 * the <code>Page</code> itself.
 * <p>
 * The pager keeps a map by position of the <code>RawPage</code> object an
 * not the <code>Page</code> objects, because the <code>Page</code>
 * associated with a <code>RawPage</code> can change. Specifically, the
 * <code>Page</code> associated with a <code>RawPage</code> can be
 * promoted from a <code>RelocatablePage</code> to a subclass of
 * <code>RelocatablePage</code>
 * <p>
 * The circular reference is necessary. The <code>Page</code>
 * implementation will need read and write the <code>RawPage</code>.The
 * <code>RawPage</code> is the constant that is mapped to the position in
 * the <code>Pager</code>. The <code>RawPage</code> is the lock on the
 * region in the file and it's associated <code>Page</code> can change.
 * 
 * @see Pager#getPage
 */
interface Page
{
    /**
     * Return the underlying raw page associated with this page.
     * 
     * @return The raw page.
     */
    public RawPage getRawPage();

    /**
     * Initialize the raw page to the specific interpretation implemented by
     * this page. This method is called from within the <code>Pager</code>
     * when a new raw page is allocated. The specified raw page will
     * subsequently be returned by {@link #getRawPage getRawPage}.
     * 
     * @param rawPage
     *            The raw page.
     * @param dirtyPages
     *            The collection of dirty pages.
     * @see Pager#getPage
     */
    public void create(RawPage rawPage, DirtyPageMap dirtyPages);

    /**
     * Load this specific interpretation from the specified the raw page.
     * This method is called from within the {@link Pager#getPage
     * getPage} method of the <code>Pager</code> when a page is loaded
     * from the file. The specified <code>RawPage</code> will be
     * subsequently returned by {@link #getRawPage getRawPage}.
     * 
     * @param rawPage
     *            The raw page.
     * @see Pager#getPage
     */
    public void load(RawPage rawPage);
    
    public void checksum(Checksum checksum);
    
    public boolean verifyChecksum(RawPage rawPage, Recovery recovery);
}