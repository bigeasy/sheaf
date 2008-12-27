package com.goodworkalan.pack;

import java.net.URI;

public interface Schema
    {
        public int getPageSize();
        
        public int getAlignment();
        
//        public int getInternalJournalCount();
        
        public Disk getDisk();
        
        public long getStaticPageAddress(URI uri);
    }