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
import org.datanucleus.PropertyNames;
import org.datanucleus.Configuration;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.query.QueryUtils;
import org.datanucleus.store.query.cache.AbstractQueryResultsCache;

import cacheonix.Cacheonix;
import cacheonix.cache.Cache;

/**
 * Implementation of a query results cache using Cacheonix.
 */
public class CacheonixQueryResultCache extends AbstractQueryResultsCache
{
    private static final long serialVersionUID = -7951373996155521704L;

    Cacheonix cacheManager;

    /** Fallback class when we can't derive the class name from the identity (composite id). */
    Cache<Serializable, Serializable> queryCache;

    public CacheonixQueryResultCache(NucleusContext nucleusCtx)
    {
        super(nucleusCtx);

        Configuration conf = nucleusCtx.getConfiguration();

        String configFile = conf.getStringProperty(PropertyNames.PROPERTY_CACHE_QUERYRESULTS_CONFIG_FILE);

        cacheManager = (configFile == null) ? Cacheonix.getInstance() : Cacheonix.getInstance(configFile);

        queryCache = cacheManager.getCache(cacheName);
    }

    public void close()
    {
        evictAll();
        cacheManager.shutdown();
    }

    public void evict(Class candidate)
    {
        // Not supported. Do nothing. TODO Support this
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

        if (expiryMillis > 0)
        {
            queryCache.put(queryKey, (Serializable)results, expiryMillis);
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