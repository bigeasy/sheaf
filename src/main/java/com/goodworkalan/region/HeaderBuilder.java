package com.goodworkalan.region;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

// TODO Document.
public class HeaderBuilder<K>
{
    // TODO Document.
    private int offset;
    
    // TODO Document.
    private final Map<K, List<Integer>> offsets;
    
    // TODO Document.
    public HeaderBuilder()
    {
        this.offsets = new HashMap<K, List<Integer>>();
    }
    
    // TODO Document.
    public void addField(K key, int length)
    {
        List<Integer> mapping = new ArrayList<Integer>(2);
        mapping.add(offset);
        mapping.add(length);
        offsets.put(key, mapping);
        offset += length;
    }
    
    // TODO Document.
    public Header<K> newHeader(long position, ByteBuffer byteBuffer)
    {
        if (byteBuffer.capacity() < offset)
        {
            throw new IllegalArgumentException();
        }
        return new Header<K>(position, offsets, byteBuffer, new ReentrantLock());
    }
    
    // TODO Document.
    public Header<K> newHeader(long position)
    {
        ByteBuffer byteBuffer = ByteBuffer.allocate(offset);
        return newHeader(position, byteBuffer);
    }
}
