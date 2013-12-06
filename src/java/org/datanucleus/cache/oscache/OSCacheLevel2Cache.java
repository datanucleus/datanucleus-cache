/**********************************************************************
Copyright (c) 2005 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
    ...
**********************************************************************/
package org.datanucleus.cache.oscache;

import java.util.Collection;
import java.util.Iterator;

import org.datanucleus.NucleusContext;
import org.datanucleus.cache.AbstractLevel2Cache;
import org.datanucleus.cache.CachedPC;

import com.opensymphony.oscache.base.NeedsRefreshException;
import com.opensymphony.oscache.general.GeneralCacheAdministrator;

/**
 * Simple implementation of a plugin for use of OSCache caching product with DataNucleus. 
 * Please refer to <a href="http://www.opensymphony.com/oscache/">OpenSymphony OSCache</a>
 * for full details of their product.
 */
public class OSCacheLevel2Cache extends AbstractLevel2Cache
{
    /** The cache manager */
    private final GeneralCacheAdministrator cache;

    /**
     * Constructor.
     * @param nucleusCtx Context
     */
    public OSCacheLevel2Cache(NucleusContext nucleusCtx)
    {
        super(nucleusCtx);

        cache = new GeneralCacheAdministrator();
        // TODO Implement refreshPeriod, and cron capability
    }

    /**
     * Method to close the cache when no longer needed. Provides a hook to release resources etc.
     */
    public void close()
    {
        if (clearAtClose)
        {
            evictAll();
        }
        cache.destroy();
    }

    /**
     * Accessor for whether the cache contains the specified id.
     * @see org.datanucleus.cache.Level2Cache#containsOid(java.lang.Object)
     */
    public boolean containsOid(Object oid)
    {
        return (get(oid) != null);
    }

    /**
     * Accessor for an object in the cache.
     * @see org.datanucleus.cache.Level2Cache#get(java.lang.Object)
     */
    public CachedPC get(Object oid)
    {
        try
        {
            return (CachedPC)cache.getFromCache(toString(oid));
        }
        catch (NeedsRefreshException e)
        {
            cache.cancelUpdate(toString(oid));
            return null;
        }
    }

    /**
     * Accessor for the size of the cache.
     * @see org.datanucleus.cache.Level2Cache#getSize()
     */
    public int getSize()
    {
        throw new UnsupportedOperationException("getSize() method not yet supported by OSCache plugin");
    }

    /**
     * Method to add an object to the cache under its id
     * @param oid The identity
     * @param pc The cacheable object
     * @return Previous object stored under this id
     */
    public CachedPC put(Object oid, CachedPC pc)
    {
        if (oid == null || pc == null)
        {
            return null;
        }

        if (containsOid(oid))
        {
            // Make sure that OSCache removes the old value
            evict(oid);
        }

        cache.putInCache(toString(oid), pc);
        return pc;
    }

    /**
     * Evict the parameter instance from the second-level cache.
     * @param oid the object id of the instance to evict.
     */
    public void evict(Object oid)
    {
        cache.flushEntry(toString(oid));
    }

    /**
     * Evict the parameter instances from the second-level cache.
     * All instances in the PersistenceManager's cache are evicted from the second-level cache.
     */
    public void evictAll()
    {
        cache.flushAll();
    }

    /**
     * Evict the parameter instances from the second-level cache.
     * @param pcClass the class of instances to evict
     * @param subclasses if true, evict instances of subclasses also
     */
    public void evictAll(Class pcClass, boolean subclasses)
    {
        throw new UnsupportedOperationException("evict(Class, boolean) method not yet supported by OSCache plugin");
    }

    /**
     * Evict the parameter instances from the second-level cache.
     * @param oids the object ids of the instance to evict.
     */
    public void evictAll(Collection oids)
    {
        if (oids == null)
        {
            return;
        }

        Iterator iter = oids.iterator();
        while (iter.hasNext())
        {
            evict(iter.next());
        }
    }

    /**
     * Evict the parameter instances from the second-level cache.
     * @param oids the object ids of the instance to evict.
     */
    public void evictAll(Object[] oids)
    {
        if (oids == null)
        {
            return;
        }

        for (int i=0;i<oids.length;i++)
        {
            evict(oids[i]);
        }
    }

    /**
     * Convert from a key to its String form (the key qualified by the cache name).
     * @param key the key
     * @return the String to key with
     */
    private String toString(Object key)
    {
        if (cacheName != null)
        {
            return String.valueOf(key) + '.' + cacheName;
        }
        else
        {
            return String.valueOf(key);
        }
    }
}