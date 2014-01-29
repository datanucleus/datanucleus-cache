/**********************************************************************
Copyright (c) 2005 Roland Szabo and others. All rights reserved.
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
package org.datanucleus.cache.ehcache;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.datanucleus.NucleusContext;
import org.datanucleus.Configuration;
import org.datanucleus.cache.AbstractLevel2Cache;
import org.datanucleus.cache.CachedPC;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.identity.OID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.Localiser;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.ObjectExistsException;

/**
 * Plugin for EHCache that allows the user to use different caches for different classes.
 * Based on the EhcacheLevel2Cache class.
 */
public class EhcacheClassBasedLevel2Cache extends AbstractLevel2Cache
{
    /** Localisation of messages. */
    private static final Localiser LOCALISER_EHCACHE = Localiser.getInstance("org.datanucleus.cache.ehcache.Localisation_ehcache",EhcacheClassBasedLevel2Cache.class.getClassLoader());

    private final CacheManager cacheManager;

    private final Cache defaultCache;

    private final Map<String, Cache> caches = new HashMap();

    /**
     * Constructor.
     * @param nucleusCtx Context
     */
    public EhcacheClassBasedLevel2Cache(NucleusContext nucleusCtx)
    {
        super(nucleusCtx);

        Configuration conf = nucleusCtx.getConfiguration();
        String configFile = conf.getStringProperty("datanucleus.cache.level2.configurationFile");
        try
        {
            if (configFile == null)
            {
                cacheManager = CacheManager.create();
            }
            else
            {
                cacheManager = CacheManager.create(CacheManager.class.getResource(configFile));
            }
        }
        catch (CacheException e)
        {
            throw new NucleusException(LOCALISER_EHCACHE.msg("Cache.EHCache.CacheManagerInitialiseFails", e));
        }

        if (!cacheManager.cacheExists(cacheName))
        {
            try
            {
                cacheManager.addCache(cacheName);
            }
            catch (IllegalStateException e)
            {
                NucleusLogger.CACHE.warn("Error creating Cache : " + e.getMessage());
                throw new NucleusException(LOCALISER_EHCACHE.msg("Cache.EHCache.CreateDefaultFails", e));
            }
            catch (ObjectExistsException e)
            {
                NucleusLogger.CACHE.warn("Error creating Cache : " + e.getMessage());
                throw new NucleusException(LOCALISER_EHCACHE.msg("Cache.EHCache.CreateDefaultFails", e));
            }
            catch (CacheException e)
            {
                NucleusLogger.CACHE.warn("Error creating Cache : " + e.getMessage());
                throw new NucleusException(LOCALISER_EHCACHE.msg("Cache.EHCache.CreateDefaultFails", e));
            }
        }
        defaultCache = cacheManager.getCache(cacheName);
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
        cacheManager.shutdown();
    }

    /**
     * Accessor for whether an object with the specified id is in the cache
     * @param oid The object id
     * @return Whether it is in the cache
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

    /**
     * Accessor for an object from the cache.
     * @param oid The Object ID
     * @return The L2 cacheable object
     */
    public CachedPC get(Object oid)
    {
        try
        {
            Element element = getCacheForId(oid).get((Serializable) oid);
            if (element == null)
            {
                return null;
            }
            return toPC(element);
        }
        catch (IllegalStateException e)
        {
            NucleusLogger.CACHE.warn("Error invoking Cache.get : " + e.getMessage());
        }
        catch (CacheException e)
        {
            NucleusLogger.CACHE.warn("Error invoking Cache.get : " + e.getMessage());
        }
        return null;
    }

    /**
     * Convert from Element to a cacheable object
     * @param object the Element
     * @return the cacheable object
     */
    private CachedPC toPC(Element object)
    {
        return (CachedPC)object.getObjectValue();
    }

    /**
     * Convert from PersistenceCapable to Element
     * @param oid the id
     * @param object the PersistenceCapable
     * @return the Element
     */
    private Element toElement(Object oid, CachedPC object)
    {
        return new Element((Serializable) oid, object);
    }

    /**
     * Accessor for the size of the cache.
     * @see org.datanucleus.cache.Level2Cache#getSize()
     */
    public int getSize()
    {
        try
        {
            int size = defaultCache.getSize();
            for (Iterator<Cache> i = caches.values().iterator(); i.hasNext();)
            {
                size += i.next().getSize();
            }
            return size;
        }
        catch (IllegalStateException e)
        {
            NucleusLogger.CACHE.warn("Error invoking Cache.getSize : " + e.getMessage());
        }
        catch (CacheException e)
        {
            NucleusLogger.CACHE.warn("Error invoking Cache.getSIze : " + e.getMessage());
        }
        return 0;
    }

    /**
     * Method to add an object to the cache under its id
     * @param oid The identity
     * @param pc The cacheable object
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

        getCacheForId(oid).put(toElement(oid, pc));

        return pc;
    }

    /**
     * Evict the parameter instance from the second-level cache.
     * @param oid the object id of the instance to evict.
     */
    public void evict(Object oid)
    {
        Object pc = get(oid);
        if (pc != null)
        {
            getCacheForId(oid).remove((Serializable)oid);
        }
    }

    /**
     * Evict the parameter instances from the second-level cache.
     * All instances in the PersistenceManager's cache are evicted
     * from the second-level cache.
     */
    public void evictAll()
    {
        try
        {
            for (Iterator<Cache> i = caches.values().iterator(); i.hasNext();)
            {
                i.next().removeAll();
            }
            defaultCache.removeAll();
        }
        catch (Exception e)
        {
            NucleusLogger.CACHE.warn("Error invoking Cache.clear : " + e.getMessage());
        }
    }

    /**
     * Evict the parameter instances from the second-level cache.
     * @param pcClass the class of instances to evict
     * @param subclasses if true, evict instances of subclasses also
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
            cache.removeAll();
        }
        else
        {
            cache = defaultCache;
            Iterator keyIter = cache.getKeys().iterator();
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

        for (int i = 0; i < oids.length; i++)
        {
            evict(oids[i]);
        }
    }

    private Cache getCacheForClass(String pcClassName)
    {
        Cache cache = caches.get(pcClassName);
        if (cache == null)
        {
            if (NucleusLogger.CACHE.isDebugEnabled())
            {
                NucleusLogger.CACHE.debug(LOCALISER_EHCACHE.msg("Cache.EHCache.Initialising", pcClassName));
            }
            if (cacheManager.cacheExists(pcClassName))
            {
                if (NucleusLogger.CACHE.isDebugEnabled())
                {
                    NucleusLogger.CACHE.debug(LOCALISER_EHCACHE.msg("Cache.EHCache.Exists", pcClassName));
                }
                cache = cacheManager.getCache(pcClassName);
            }
            else
            {
                // if defaultCache is null, there should be a warning here for the user,
                // that he either needs to define the cache for the class, or give a default                
                if (NucleusLogger.CACHE.isDebugEnabled())
                {
                    NucleusLogger.CACHE.debug(LOCALISER_EHCACHE.msg("Cache.EHCache.CacheDoesntExist"));
                }
                if (defaultCache == null)
                {
                    NucleusLogger.CACHE.error(LOCALISER_EHCACHE.msg("Cache.EHCache.CacheDoestExistNoDefault", pcClassName));
                }
                cache = defaultCache;
            }
            caches.put(pcClassName, cache);
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