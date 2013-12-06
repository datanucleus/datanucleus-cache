/**********************************************************************
Copyright (c) 2011 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.cache.cacheonix;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.datanucleus.NucleusContext;
import org.datanucleus.PersistenceConfiguration;
import org.datanucleus.cache.AbstractLevel2Cache;
import org.datanucleus.cache.CachedPC;
import org.datanucleus.identity.OID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.util.NucleusLogger;

import cacheonix.Cacheonix;
import cacheonix.cache.Cache;

/**
 * Plugin for Cacheonix that allows the user to use different caches for different classes.
 */
public class CacheonixLevel2Cache extends AbstractLevel2Cache
{
    Cacheonix cacheManager;

    /** Map of cache keyed by the class name (one cache per class). */
    final Map<String, Cache> caches = new HashMap();

    /** Fallback class when we can't derive the class name from the identity (composite id). */
    Cache<Serializable, Serializable> defaultCache;

    /**
     * @param nucleusCtx
     */
    public CacheonixLevel2Cache(NucleusContext nucleusCtx)
    {
        super(nucleusCtx);

        PersistenceConfiguration conf = nucleusCtx.getPersistenceConfiguration();
        String configFile = conf.getStringProperty("datanucleus.cache.level2.configurationFile");
        if (configFile == null)
        {
            cacheManager = Cacheonix.getInstance();
        }
        else
        {
            cacheManager = Cacheonix.getInstance(configFile);
        }

        defaultCache = cacheManager.getCache(cacheName);
        if (defaultCache == null)
        {
            defaultCache = cacheManager.createCache(cacheName);
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.cache.Level2Cache#close()
     */
    public void close()
    {
        if (clearAtClose)
        {
            evictAll();
        }
        cacheManager.shutdown();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.cache.Level2Cache#evict(java.lang.Object)
     */
    public void evict(Object oid)
    {
        Object pc = get(oid);
        if (pc != null)
        {
            getCacheForId(oid).remove((Serializable)oid);
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.cache.Level2Cache#evictAll()
     */
    public void evictAll()
    {
        for (Iterator i = caches.values().iterator(); i.hasNext();)
        {
            ((Cache) i.next()).clear();
        }
        defaultCache.clear();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.cache.Level2Cache#evictAll(java.lang.Object[])
     */
    public void evictAll(Object[] oids)
    {
        if (oids == null)
        {
            return;
        }

        for (int i = 0; i < oids.length; i++)
        {
            evict(oids[i]);
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.cache.Level2Cache#evictAll(java.util.Collection)
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

    /* (non-Javadoc)
     * @see org.datanucleus.cache.Level2Cache#evictAll(java.lang.Class, boolean)
     */
    public void evictAll(Class pcClass, boolean subclasses)
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
        Cache cache = null;
        if (cmd.usesSingleFieldIdentityClass() || cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            cache = caches.get(cmd.getFullClassName());
            cache.clear();
        }
        else
        {
            cache = defaultCache;
            Iterator keyIter = cache.keySet().iterator();
            while (keyIter.hasNext())
            {
                Object key = keyIter.next();
                if (cmd.getIdentityType() == IdentityType.APPLICATION)
                {
                    String targetClassName = nucleusCtx.getApiAdapter().getTargetClassNameForSingleFieldIdentity(key);
                    if (className.equals(targetClassName))
                    {
                        keyIter.remove();
                    }
                }
                else if (cmd.getIdentityType() == IdentityType.DATASTORE && key instanceof OID)
                {
                    OID oid = (OID)key;
                    if (className.equals(oid.getPcClass()))
                    {
                        keyIter.remove();
                    }
                }
            }
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.cache.Level2Cache#getSize()
     */
    public int getSize()
    {
        int size = defaultCache.size();
        for (Iterator i = caches.values().iterator(); i.hasNext();)
        {
            size += ((Cache) i.next()).size();
        }
        return size;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.cache.Level2Cache#get(java.lang.Object)
     */
    public CachedPC get(Object oid)
    {
        return (CachedPC) getCacheForId(oid).get(oid);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.cache.Level2Cache#put(java.lang.Object, org.datanucleus.cache.CachedPC)
     */
    public CachedPC put(Object oid, CachedPC pc)
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
            if (timeout > 0)
            {
                getCacheForId(oid).put((Serializable) oid, pc, timeout);
            }
            else
            {
                getCacheForId(oid).put(oid, pc);
            }
        }
        catch (RuntimeException re)
        {
            // Not cached due to some problem. Not serializable?
            NucleusLogger.CACHE.debug("Object with id " + oid +" not cached due to : " + re.getMessage());
        }

        return pc;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.cache.Level2Cache#containsOid(java.lang.Object)
     */
    public boolean containsOid(Object oid)
    {
        try
        {
            return (get(oid) != null);
        }
        catch (IllegalStateException e)
        {
            NucleusLogger.CACHE.warn("Error invoking Cache.containsOid : " + e.getMessage());
        }

        return false;
    }

    private Cache<Serializable, Serializable> getCacheForClass(String cacheName)
    {
        Cache cache = (Cache) caches.get(cacheName);
        if (cache == null)
        {
            cache = cacheManager.getCache(cacheName);
            if (cache == null)
            {
                cache = cacheManager.createCache(cacheName);
            }
            caches.put(cacheName, cache);
        }
        return cache;
    }

    private Cache getCacheForId(Object id)
    {
        if (nucleusCtx.getApiAdapter().isSingleFieldIdentity(id))
        {
            String targetClassName = nucleusCtx.getApiAdapter().getTargetClassNameForSingleFieldIdentity(id);
            return getCacheForClass(targetClassName);
        }
        if (id instanceof OID)
        {
            return getCacheForClass(((OID) id).getPcClass());
        }
        return defaultCache;
    }
}