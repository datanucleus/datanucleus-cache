/**********************************************************************
Copyright (c) 2009 Erik Bengtson and others. All rights reserved.
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
package org.datanucleus.cache.jcache;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import net.sf.jsr107cache.Cache;
import net.sf.jsr107cache.CacheException;
import net.sf.jsr107cache.CacheManager;

import org.datanucleus.NucleusContext;
import org.datanucleus.cache.AbstractLevel2Cache;
import org.datanucleus.cache.CachedPC;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.identity.OID;
import org.datanucleus.identity.SingleFieldId;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.util.NucleusLogger;

/**
 * Simple implementation of a plugin for use of early version of javax.cache (0.2 and earlier) product with DataNucleus. 
 */
public class JCacheLevel2Cache extends AbstractLevel2Cache
{
    /** The cache to use. */
    private final Cache cache;

    /**
     * Constructor.
     * @param nucleusCtx Context
     */
    public JCacheLevel2Cache(NucleusContext nucleusCtx)
    {
        super(nucleusCtx);

        try
        {
            Cache tmpcache = CacheManager.getInstance().getCache(cacheName);
            if (tmpcache == null)
            {
                Map props = new HashMap();
                if (timeout > 0)
                {
                    // Used by Google Memcache to set expiration timeout in millis
                    props.put(1, timeout);
                }
                cache = CacheManager.getInstance().getCacheFactory().createCache(props);
                CacheManager.getInstance().registerCache(cacheName, cache);
            }
            else
            {
                cache = tmpcache;
            }
        }
        catch (CacheException e)
        {
            throw new NucleusException("Error creating cache", e);
        }
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
        return (CachedPC) cache.get(oid);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.cache.AbstractLevel2Cache#getAll(java.util.Collection)
     */
    @Override
    public Map<Object, CachedPC> getAll(Collection oids)
    {
        try
        {
            return cache.getAll(oids);
        }
        catch (CacheException ce)
        {
            throw new NucleusException("Exception occurred during get from cache", ce);
        }
    }

    /**
     * Accessor for the size of the cache.
     * @see org.datanucleus.cache.Level2Cache#getSize()
     */
    public int getSize()
    {
        // TODO Implement this
        throw new UnsupportedOperationException("size() method not supported by this plugin");
    }

    /**
     * Accessor for whether the cache is empty
     * @see org.datanucleus.cache.Level2Cache#isEmpty()
     */
    public boolean isEmpty()
    {
        return getSize() == 0;
    }

    /**
     * Method to add an object to the cache under its id
     * @param oid The identity
     * @param pc The cacheable object
     */
    public synchronized CachedPC put(Object oid, CachedPC pc)
    {
        if (oid == null || pc == null)
        {
            return null;
        }
        else if (maxSize >= 0 && getSize() == maxSize)
        {
            return null;
        }

        try
        {
            cache.put(oid, pc);
        }
        catch (RuntimeException re)
        {
            // Not cached due to some problem. Not serializable?
            NucleusLogger.CACHE.info("Object with id " + oid +" not cached due to : " + re.getMessage());
        }
        return pc;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.cache.AbstractLevel2Cache#putAll(java.util.Map)
     */
    @Override
    public void putAll(Map<Object, CachedPC> objs)
    {
        if (objs == null)
        {
            return;
        }

        try
        {
            cache.putAll(objs);
        }
        catch (RuntimeException re)
        {
            // Not cached due to some problem. Not serializable?
            NucleusLogger.CACHE.info("Objects not cached due to : " + re.getMessage());
        }
    }

    /**
     * Evict the parameter instance from the second-level cache.
     * @param oid the object id of the instance to evict.
     */
    public synchronized void evict(Object oid)
    {
        cache.remove(oid);
    }

    /**
     * Evict the parameter instances from the second-level cache.
     * All instances are evicted from the second-level cache.
     */
    public synchronized void evictAll()
    {
        cache.clear();
    }

    /**
     * Evict the parameter instances from the second-level cache.
     * @param oids the object ids of the instance to evict.
     */
    public synchronized void evictAll(Collection oids)
    {
        if (oids == null)
        {
            return;
        }

        for (Object oid : oids)
        {
            cache.remove(oid);
        }
    }

    /**
     * Evict the parameter instances from the second-level cache.
     * @param oids the object ids of the instance to evict.
     */
    public synchronized void evictAll(Object[] oids)
    {
        if (oids == null)
        {
            return;
        }

        for (int i=0;i<oids.length;i++)
        {
            cache.remove(oids[i]);
        }
    }

    /**
     * Evict the parameter instances from the second-level cache.
     * @param pcClass the class of instances to evict
     * @param subclasses if true, evict instances of subclasses also
     */
    public synchronized void evictAll(Class pcClass, boolean subclasses)
    {
        if (!nucleusCtx.getApiAdapter().isPersistable(pcClass))
        {
            return;
        }

        evictAllOfClass(pcClass.getName());
        if (subclasses)
        {
            String[] subclassNames = nucleusCtx.getMetaDataManager().getSubclassesForClass(pcClass.getName(), true);
            if (subclassNames != null)
            {
                for (int i=0;i<subclassNames.length;i++)
                {
                    evictAllOfClass(subclassNames[i]);
                }
            }
        }

    }

    void evictAllOfClass(String className)
    {
        AbstractClassMetaData cmd =
            nucleusCtx.getMetaDataManager().getMetaDataForClass(className, nucleusCtx.getClassLoaderResolver(null));
        Iterator<Cache.Entry> entryIter = cache.entrySet().iterator();
        while (entryIter.hasNext())
        {
            Cache.Entry entry = entryIter.next();
            Object key = entry.getKey();
            if (cmd.getIdentityType() == IdentityType.APPLICATION)
            {
                String targetClassName = ((SingleFieldId)key).getTargetClassName();
                if (className.equals(targetClassName))
                {
                    entryIter.remove();
                }
            }
            else if (cmd.getIdentityType() == IdentityType.DATASTORE && key instanceof OID)
            {
                String targetClassName = ((OID)key).getTargetClassName();
                if (className.equals(targetClassName))
                {
                    entryIter.remove();
                }
            }
        }
    }
}