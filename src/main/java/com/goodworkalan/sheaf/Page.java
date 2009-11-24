package com.goodworkalan.sheaf;

/**
 * Derived classes implement the address, block, journal formats of an
 * underlying page in a Pack file. The <code>Page</code> interface is a
 * specialized application of an underlying page. It is the strategy participant
 * in the strategy design pattern. Each page has an underlying {@link RawPage}.
 * <p>
 * <code>RawPage</code> contains the basic position and byte buffer page
 * attributes and maintains a set of invalid regions. A <code>Page</code>
 * implementation interprets the raw page as a specific page type.
 * <h4>Frequent Instantiation and Two-Step Initialization</h4>
 * <p>
 * When we request a page from the the pager, we give it an implementation of
 * <code>Page</code> that represents the type request. If the raw page exists in
 * the pager and it is associated with the type of page requested, that page is
 * returned. If it is not in the pager, or if the page associated with the raw
 * page is a super class of the page requested, the raw page is associated with
 * the <code>Page</code> implementation given.
 * <p>
 * This means that we create a lot of instances of type <code>Page</code> that
 * are not initialized. They are not initialized because they are not needed.
 * The pager has the right type of <code>Page</code>. They are simply forgotten
 * and left for the garbage collector. For this reason, it is important that
 * implementations of <code>Page</code> have simple default constructors.
 * <p>
 * <code>Page</code> instances are not initialized by their default
 * constructors. They are actually initialized by a call to either
 * {@link #create create} or {@link #load load}.
 * <h4>Circular RawPage and Page Reference</h4>
 * <p>
 * The circular reference between <code>Page</code> and <code>RawPage</code> is
 * necessary. <code>RawPage</code> is used as the mutex for changes to the
 * underlying byte buffer as well as to the data members of the
 * <code>Page</code> itself. All the <code>Page</code> implementations use the
 * associated <code>RawPage</code> as the lock object in synchronized blocks as
 * opposed to using synchronized methods on the <code>Page</code> itself.
 * <p>
 * The pager keeps a map by position of the <code>RawPage</code> object an not
 * the <code>Page</code> objects, because the <code>Page</code> associated with
 * a <code>RawPage</code> can change. Specifically, the <code>Page</code>
 * associated with a <code>RawPage</code> can be promoted from a
 * <code>RelocatablePage</code> to a subclass of <code>RelocatablePage</code>
 * 
 * @see Sheaf#getPage(long, Class, Page)
 * @see Sheaf#setPage(long, Class, Page, DirtyPageSet)
 */
public class Page {
    /** The underlying raw page. */
    private RawPage rawPage;

    /**
     * Set the underlying raw page.
     * <p>
     * This can only be set by the <code>Sheaf</code> that maintains the
     * <code>RawPage</code>.
     * 
     * @param rawPage
     */
    void setRawPage(RawPage rawPage) {
        this.rawPage = rawPage;
    }

    /**
     * Return the underlying raw page associated with this page.
     * 
     * @return The raw page.
     */
    public RawPage getRawPage() {
        return rawPage;
    }

    /**
     * Initialize the raw page to the specific interpretation implemented by
     * this page. This method is called from within the <code>Pager</code> when
     * a new raw page is allocated. The specified raw page will subsequently be
     * returned by {@link #getRawPage() getRawPage}.
     * 
     * @param dirtyPages
     *            The collection of dirty pages.
     * @see Sheaf#setPage(long, Class, Page, DirtyPageSet)
     */
    public void create(DirtyPageSet dirtyPages) {
    }

    /**
     * Load this specific interpretation from the specified the raw page. This
     * method is called from within the {@link Sheaf#getPage getPage} method of
     * the <code>Pager</code> when a page is loaded from the file. The specified
     * <code>RawPage</code> will be subsequently returned by
     * {@link #getRawPage() getRawPage}.
     * 
     * @see Sheaf#getPage(long, Class, Page)
     */
    public void load() {
    }
}
