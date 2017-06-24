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
package org.datanucleus.cache.xmemcached;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.utils.AddrUtil;

import org.datanucleus.NucleusContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.Configuration;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.query.QueryUtils;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.query.cache.QueryResultsCache;
import org.datanucleus.util.NucleusLogger;

/**
 * Plugin using xmemcached implementation of "memcached" as a query results cache.
 */
public class XmemcachedQueryResultCache implements QueryResultsCache
{
    private static final long serialVersionUID = 8865474095320516082L;

    private MemcachedClient client;

    /** Prefix (for uniqueness) to ensure sharing with other xmemcached objects. */
    private String keyPrefix = "datanucleus-query:";

    private int expireSeconds = 0;

    public XmemcachedQueryResultCache(NucleusContext nucleusCtx)
    {
        Configuration conf = nucleusCtx.getConfiguration();

        String keyPrefix = conf.getStringProperty("datanucleus.cache.query.memcached.keyprefix");
        if (keyPrefix != null)
        {
            this.keyPrefix = keyPrefix;
        }

        if (conf.hasPropertyNotNull(PropertyNames.PROPERTY_CACHE_QUERYRESULTS_EXPIRY_MILLIS))
        {
            long expireMillis = conf.getIntProperty(PropertyNames.PROPERTY_CACHE_QUERYRESULTS_EXPIRY_MILLIS);
            expireSeconds = (int)expireMillis/1000;
        }

        String servers = conf.getStringProperty("datanucleus.cache.level2.memcached.servers");
        MemcachedClientBuilder builder = new XMemcachedClientBuilder(AddrUtil.getAddresses(servers));
        try
        {
            client = builder.build();
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
        try
        {
            client.flushAll();
            client.shutdown();
        }
        catch (Exception e)
        {
            NucleusLogger.CACHE.error("Exception caught shutting down cache", e);
        }
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
        try
        {
            client.delete(keyPrefix + QueryUtils.getKeyForQueryResultsCache(query, null));
        }
        catch (Exception e)
        {
            throw new NucleusException("Exception evicting entry from xmemcached", e);
        }
        // TODO Delete all entries for this query (with any possible set of params) i.e key starting baseKey
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#evict(org.datanucleus.store.query.Query, java.util.Map)
     */
    public void evict(Query query, Map params)
    {
        try
        {
            client.delete(keyPrefix + QueryUtils.getKeyForQueryResultsCache(query, params));
        }
        catch (Exception e)
        {
            throw new NucleusException("Exception evicting entry from xmemcached", e);
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#evictAll()
     */
    public void evictAll()
    {
        try
        {
            client.flushAll();
        }
        catch (Exception e)
        {
            throw new NucleusException("Exception evicting entries from xmemcached", e);
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#get(java.lang.String)
     */
    public List<Object> get(String queryKey)
    {
        try
        {
            return (List<Object>)client.get(keyPrefix + queryKey);
        }
        catch (Exception e)
        {
            throw new NucleusException("Exception thrown in retrieval from xmemcached", e);
        }
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

        try
        {
            client.set(keyPrefix + queryKey, expireSeconds, results);
        }
        catch (Exception e)
        {
            throw new NucleusException("Exception thrown in persistence to xmemcached", e);
        }

        return results;
    }
}