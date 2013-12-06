/**********************************************************************
Copyright (c) 2004 Andy Jefferson and others. All rights reserved.
This program and the accompanying materials are made available under 
the terms of the JPOX License v1.0 which accompanies this distribution.

Contributors:
    ...
**********************************************************************/
package com.tangosol.net;

/**
 * Stub for Tangosol's NamedCache class to allow building for calls to it.
 * @version $Revision: 1.1 $
 */
public class NamedCache
{
    public Object get(Object oid)
    {
        return null;
    }
    
    public Object put(Object oid, Object value)
    {
        return null;
    }
    
    public void clear()
    {
    }
    
    public int size()
    {
        return 0;
    }
    
    public boolean isEmpty()
    {
        return true;
    }
    
    public boolean containsKey(Object obj)
    {
        return true;
    }
    
    public boolean containsValue(Object obj)
    {
        return true;
    }
    
    public Object remove(Object obj)
    {
        return null;
    }

    public boolean lock(Object key)
    {
        return true;
    }

    public boolean lock(Object key, int wait)
    {
        return true;
    }

    public boolean unlock(Object key)
    {
        return true;
    }
}