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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.NucleusContext;
import org.datanucleus.PersistenceConfiguration;
import org.datanucleus.query.QueryUtils;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.query.cache.QueryResultsCache;
import org.datanucleus.util.NucleusLogger;

import cacheonix.Cacheonix;
import cacheonix.cache.Cache;

/**
 * Implementation of a query results cache using Cacheonix.
 */
public class CacheonixQueryResultCache implements QueryResultsCache
{
    Cacheonix cacheManager;

    /** User-provided timeout for cache object expiration (milliseconds). */
    long timeout = -1;

    /** Fallback class when we can't derive the class name from the identity (composite id). */
    Cache<Serializable, Serializable> queryCache;

    public CacheonixQueryResultCache(NucleusContext nucleusCtx)
    {
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

        if (conf.hasProperty("datanucleus.cache.level2.timeout"))
        {
            timeout = conf.getIntProperty("datanucleus.cache.level2.timeout");
        }

        String cacheName = conf.getStringProperty("datanucleus.cache.queryResults.cacheName");
        if (cacheName == null)
        {
            NucleusLogger.CACHE.warn("No 'datanucleus.cache.queryResults.cacheName' specified so using name of 'DataNucleus-Query'");
            cacheName = "datanucleus-query";
        }
        queryCache = cacheManager.getCache(cacheName);
    }

    public void close()
    {
        evictAll();
        cacheManager.shutdown();
    }

    public void evict(Class candidate)
    {
        throw new UnsupportedOperationException("Not yet supported");
    }

    public void evict(Query query)
    {
        String baseKey = QueryUtils.getKeyForQueryResultsCache(query, null);
        Iterator iter = queryCache.keySet().iterator();
        while (iter.hasNext())
        {
            String key = (String)iter.next();
            if (key.startsWith(baseKey))
            {
                iter.remove();
            }
        }
    }

    public void evict(Query query, Map params)
    {
        String key = QueryUtils.getKeyForQueryResultsCache(query, params);
        queryCache.remove(key);
    }

    public void evictAll()
    {
        queryCache.clear();
    }

    public void pin(Query query)
    {
        throw new UnsupportedOperationException("This cache doesn't support pinning/unpinning");
    }

    public void pin(Query query, Map params)
    {
        throw new UnsupportedOperationException("This cache doesn't support pinning/unpinning");
    }

    public void unpin(Query query)
    {
        throw new UnsupportedOperationException("This cache doesn't support pinning/unpinning");
    }

    public void unpin(Query query, Map params)
    {
        throw new UnsupportedOperationException("This cache doesn't support pinning/unpinning");
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public int size()
    {
        return queryCache.size();
    }

    public List<Object> get(String queryKey)
    {
        return (List<Object>) queryCache.get(queryKey);
    }

    public List<Object> put(String queryKey, List<Object> results)
    {
        if (queryKey == null || results == null)
        {
            return null;
        }

        if (timeout > 0)
        {
            queryCache.put(queryKey, (Serializable)results, timeout);
        }
        else
        {
            queryCache.put(queryKey, (Serializable)results);
        }
        return results;
    }

    public boolean contains(String queryKey)
    {
        return (get(queryKey) != null);
    }
}