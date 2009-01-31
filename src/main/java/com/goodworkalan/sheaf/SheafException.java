package com.goodworkalan.sheaf;

import java.util.ArrayList;
import java.util.List;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

public final class SheafException
extends RuntimeException
{
    private static final long serialVersionUID = 20070821L;
    
    protected final List<Object> listOfArguments = new ArrayList<Object>(); 
    
    private final int code;
    
    public SheafException(int code)
    {
        this.code = code;
    }
    
    public SheafException(int code, Throwable cause)
    {
        super(cause);
        this.code = code;
    }
    
    public int getCode()
    {
        return code;
    }
    
    @Override
    public String getMessage()
    {
        String key = Integer.toString(code);
        ResourceBundle exceptions = ResourceBundle.getBundle("com.goodworkalan.sheaf.exceptions");
        String format;
        try
        {
            format = exceptions.getString(key);
        }
        catch (MissingResourceException e)
        {
            return key;
        }
        try
        {
            return String.format(format, listOfArguments.toArray());
        }
        catch (Throwable e)
        {
            throw new Error(key, e);
        }
    }
}