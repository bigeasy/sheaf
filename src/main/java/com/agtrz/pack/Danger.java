package com.agtrz.pack;

public final class Danger
extends RuntimeException
{
    private static final long serialVersionUID = 20070821L;
    
    private final int code;

    public Danger(int code)
    {
        super(Integer.toString(code));
        this.code = code;
    }

    public Danger(int code, Throwable cause)
    {
        super(Integer.toString(code), cause);
        this.code = code;
    }
    
    public int getCode()
    {
        return code;
    }
}