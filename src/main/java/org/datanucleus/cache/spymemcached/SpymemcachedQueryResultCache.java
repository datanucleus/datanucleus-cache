/**********************************************************************
Copyright (c) 2009 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.cache.spymemcached;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;

import org.datanucleus.NucleusContext;
import org.datanucleus.cache.xmemcached.XmemcachedQueryResultCache;
import org.datanucleus.Configuration;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.query.QueryUtils;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.query.cache.AbstractQueryResultsCache;
import org.datanucleus.util.NucleusLogger;

/**
 * Plugin using spymemcached implementation of "memcached" as a query results cache.
 */
public class SpymemcachedQueryResultCache extends AbstractQueryResultsCache
{
    private static final long serialVersionUID = 4242859249224130913L;

    private MemcachedClient client;

    /** Prefix (for uniqueness) to ensure sharing with other memcache objects. */
    private String keyPrefix = "datanucleus-query:";

    private int expirySeconds = 0;

    public SpymemcachedQueryResultCache(NucleusContext nucleusCtx)
    {
        super(nucleusCtx);

        Configuration conf = nucleusCtx.getConfiguration();

        String keyPrefix = conf.getStringProperty(XmemcachedQueryResultCache.PROPERTY_CACHE_QUERYRESULTS_MEMCACHED_KEYPREFIX);
        if (keyPrefix != null)
        {
            this.keyPrefix = keyPrefix;
        }

        expirySeconds = (int)expiryMillis/1000;

        String servers = conf.getStringProperty(XmemcachedQueryResultCache.PROPERTY_CACHE_QUERYRESULTS_MEMCACHED_SERVERS);
        try
        {
            client = new MemcachedClient(AddrUtil.getAddresses(servers));
        }
        catch (IOException e)
        {
            NucleusLogger.CACHE.error("Exception caught creating cache", e);
            throw new NucleusException("Cant create cache", e);
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#close()
     */
    public void close()
    {
        client.shutdown();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#contains(java.lang.String)
     */
    public boolean contains(String queryKey)
    {
        return get(queryKey) != null;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#evict(java.lang.Class)
     */
    public void evict(Class candidate)
    {
        // TODO Auto-generated method stub
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#evict(org.datanucleus.store.query.Query)
     */
    public void evict(Query query)
    {
        String baseKey = QueryUtils.getKeyForQueryResultsCache(query, null);
        client.delete(keyPrefix + baseKey);
        // TODO Delete all entries for this query (with any possible set of params) i.e key starting baseKey
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#evict(org.datanucleus.store.query.Query, java.util.Map)
     */
    public void evict(Query query, Map params)
    {
        String key = QueryUtils.getKeyForQueryResultsCache(query, params);
        client.delete(keyPrefix + key);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#evictAll()
     */
    public void evictAll()
    {
        client.flush();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#get(java.lang.String)
     */
    public List<Object> get(String queryKey)
    {
        return (List<Object>)client.get(keyPrefix + queryKey);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#put(java.lang.String, java.util.List)
     */
    public List<Object> put(String queryKey, List<Object> results)
    {
        if (queryKey == null || results == null)
        {
            return null;
        }

        client.set(keyPrefix + queryKey, expirySeconds, results);

        return results;
    }
}