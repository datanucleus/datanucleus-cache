/**********************************************************************
Copyright (c) 2004 Andy Jefferson and others. All rights reserved.
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
2005 Andy Jefferson - added locking of get method
2005 Andy Jefferson - added evictAll() method
    ...
**********************************************************************/
package org.datanucleus.cache.coherence;

import java.util.Collection;
import java.util.Iterator;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.NucleusContext;
import org.datanucleus.cache.AbstractLevel2Cache;
import org.datanucleus.cache.CachedPC;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;

/**
 * Simple implementation of a plugin for use of Oracles Coherence distributed caching product with 
 * DataNucleus. Please refer to <a href="http://www.tangosol.com">www.tangosol.com</a>
 * for full details of their products. This plugin simply provides a wrapper
 * to the Coherence "NamedCache" to allow its use in DataNucleus.
 */
public class CoherenceLevel2Cache extends AbstractLevel2Cache
{
    private static final long serialVersionUID = -8366039798215561285L;

    private NamedCache cache;

    /**
     * Constructor.
     * @param nucleusCtx Context
     */
    public CoherenceLevel2Cache(NucleusContext nucleusCtx)
    {
        super(nucleusCtx);

        ClassLoaderResolver clr = nucleusCtx.getClassLoaderResolver(null);

        // Use reflection to check for the Coherence cache
        ClassUtils.assertClassForJarExistsInClasspath(clr, "com.tangosol.net.CacheFactory", "coherence.jar");

        // Access the NamedCache
        cache = CacheFactory.getCache(cacheName);
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
    }

    /**
     * Accessor for the backing Coherence cache.
     * This is provided so that users can add much more elaborate control over their cache
     * in line with what Coherence provides.
     * @return The  Coherence named cache.
     */
    public NamedCache getCoherenceCache()
    {
        return cache;
    }

    /**
     * Accessor for whether the cache contains the specified id.
     * @see org.datanucleus.cache.Level2Cache#containsOid(java.lang.Object)
     */
    public boolean containsOid(Object oid)
    {
        return cache.containsKey(oid);
    }

    /**
     * Method to lock the underlying Coherence cache.
     * @param oid The key
     * @return Success indicator
     */
    public boolean lock(Object oid)
    {
        return cache.lock(oid);
    }

    /**
     * Method to lock the underlying Coherence cache for a time period.
     * @param oid The key
     * @param wait the time period (ms)
     * @return Success indicator
     */
    public boolean lock(Object oid, int wait)
    {
        return cache.lock(oid, wait);
    }

    /**
     * Method to unlock the underlying Coherence cache.
     * @param oid The key
     * @return Success indicator
     */
    public boolean unlock(Object oid)
    {
        return cache.unlock(oid);
    }

    /**
     * Accessor for an object in the cache.
     * @see org.datanucleus.cache.Level2Cache#get(java.lang.Object)
     */
    public CachedPC get(Object oid)
    {
        // Lock the cache to prevent unnecessary DB reads.
        cache.lock(oid, -1);
        try
        {
            return (CachedPC)cache.get(oid);
        }
        finally
        {
            cache.unlock(oid);
        }
    }

    /**
     * Accessor for the size of the cache.
     * @see org.datanucleus.cache.Level2Cache#getSize()
     */
    public int getSize()
    {
        return cache.size();
    }

    /**
     * Accessor for whether the cache is empty
     * @see org.datanucleus.cache.Level2Cache#isEmpty()
     */
    public boolean isEmpty()
    {
        return cache.isEmpty();
    }

    /**
     * Method to add an object to the cache under its id
     * @param oid The identity
     * @param pc The cacheable object
     * @return previous value for this identity (if any)
     */
    public CachedPC put(Object oid, CachedPC pc)
    {
        if (oid == null || pc == null)
        {
            NucleusLogger.CACHE.warn(Localiser.msg("004011"));
            return null;
        }
        else if (maxSize >= 0 && getSize() == maxSize)
        {
            return null;
        }

        return (CachedPC) cache.put(oid, pc);
    }

    /**
     * Evict the parameter instance from the second-level cache.
     * @param oid the object id of the instance to evict.
     */
    public void evict(Object oid)
    {
        cache.remove(oid);
    }

    /**
     * Evict the parameter instances from the second-level cache.
     * All instances in the PersistenceManager's cache are evicted from the second-level cache.
     */
    public void evictAll()
    {
        cache.clear();
    }

    /**
     * Evict the parameter instances from the second-level cache.
     * @param pcClass the class of instances to evict
     * @param subclasses if true, evict instances of subclasses also
     */
    public void evictAll(Class pcClass, boolean subclasses)
    {
        // Not supported. Do nothing
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
}