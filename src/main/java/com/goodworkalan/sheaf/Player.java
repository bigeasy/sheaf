package com.goodworkalan.sheaf;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;


final class Player
{
    private final Pager pager;

    private final Pointer header;

    private long entryPosition;

    private final DirtyPageSet dirtyPages;
    
    private final SortedSet<Long> setOfAddresses;
    
    private final Set<Vacuum> setOfVacuums; 
    
    private final LinkedList<Move> listOfMoves;
    
    
    public Player(Pager pager, Pointer header, DirtyPageSet dirtyPages)
    {
        ByteBuffer bytes = header.getByteBuffer();
        
        bytes.clear();
        
        this.pager = pager;
        this.header = header;
        this.entryPosition = bytes.getLong();
        this.dirtyPages = dirtyPages;
        this.setOfAddresses = new TreeSet<Long>();
        this.setOfVacuums = new HashSet<Vacuum>();
        this.listOfMoves = new LinkedList<Move>();
    }
    
    public Pager getPager()
    {
        return pager;
    }
    
    public Pointer getJournalHeader()
    {
        return header;
    }

    public DirtyPageSet getDirtyPages()
    {
        return dirtyPages;
    }
    
    public SortedSet<Long> getAddressSet()
    {
        return setOfAddresses;
    }
    
    public Set<Vacuum> getVacuumSet()
    {
        return setOfVacuums;
    }
    
    public LinkedList<Move> getMoveList()
    {
        return listOfMoves;
    }
    
    public long adjust(long position)
    {
        return pager.adjust(getMoveList(), position);
    }

    private void execute()
    {
        JournalPage journalPage = pager.getPage(entryPosition, JournalPage.class, new JournalPage());
        
        journalPage.seek(entryPosition);
        
        Operation operation = journalPage.next(); 
        while (!operation.terminate())
        {
            operation.commit(this);
            journalPage = operation.getJournalPage(this, journalPage);
            operation = journalPage.next();
        }

        entryPosition = journalPage.getJournalPosition();
    }

    public void vacuum()
    {
        execute();
    }

    public void commit()
    {
        execute();

        header.getByteBuffer().clear();
        header.getByteBuffer().putLong(0, 0L);

        dirtyPages.flush(header);

        pager.getJournalHeaders().free(header);
    }
}