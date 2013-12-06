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
import org.datanucleus.PersistenceConfiguration;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.query.QueryUtils;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.query.cache.QueryResultsCache;

/**
 * Plugin using spymemcached implementation of "memcached" as a query results cache.
 */
public class SpymemcachedQueryResultCache implements QueryResultsCache
{
    private MemcachedClient client;

    /** Prefix (for uniqueness) to ensure sharing with other memcache objects. */
    private String keyPrefix = "datanucleus-query:";

    private int expireSeconds = 0;

    public SpymemcachedQueryResultCache(NucleusContext nucleusCtx)
    {
        PersistenceConfiguration conf = nucleusCtx.getPersistenceConfiguration();
        String keyPrefix = conf.getStringProperty("datanucleus.cache.query.memcached.keyprefix");
        String servers = conf.getStringProperty("datanucleus.cache.query.memcached.servers");
        String expireStr = conf.getStringProperty("datanucleus.cache.query.memcached.expireSeconds");
        if (keyPrefix != null)
        {
            this.keyPrefix = keyPrefix;
        }

        try
        {
            client = new MemcachedClient(AddrUtil.getAddresses(servers));
            if (expireStr != null && !"".equals(expireStr))
            {
                expireSeconds = Integer.parseInt(expireStr);
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
            throw new NucleusException("Cant create cache", e);
        }
        catch (NumberFormatException ex)
        {
            throw new NucleusException("Cant create cache: Bad expireSeconds value:" + expireStr, ex);
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
    public synchronized void evict(Query query)
    {
        String baseKey = QueryUtils.getKeyForQueryResultsCache(query, null);
        client.delete(keyPrefix + baseKey);
        // TODO Delete all entries for this query (with any possible set of params) i.e key starting baseKey
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#evict(org.datanucleus.store.query.Query, java.util.Map)
     */
    public synchronized void evict(Query query, Map params)
    {
        String key = QueryUtils.getKeyForQueryResultsCache(query, params);
        client.delete(keyPrefix + key);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#evictAll()
     */
    public synchronized void evictAll()
    {
        client.flush();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#pin(org.datanucleus.store.query.Query, java.util.Map)
     */
    public void pin(Query query, Map params)
    {
        throw new UnsupportedOperationException("This cache doesn't support pinning/unpinning");
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#pin(org.datanucleus.store.query.Query)
     */
    public void pin(Query query)
    {
        throw new UnsupportedOperationException("This cache doesn't support pinning/unpinning");
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#unpin(org.datanucleus.store.query.Query, java.util.Map)
     */
    public void unpin(Query query, Map params)
    {
        throw new UnsupportedOperationException("This cache doesn't support pinning/unpinning");
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#unpin(org.datanucleus.store.query.Query)
     */
    public void unpin(Query query)
    {
        throw new UnsupportedOperationException("This cache doesn't support pinning/unpinning");
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#get(java.lang.String)
     */
    public List<Object> get(String queryKey)
    {
        return (List<Object>)client.get(keyPrefix + queryKey);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#isEmpty()
     */
    public boolean isEmpty()
    {
        throw new UnsupportedOperationException("isEmpty() method not supported by spymemcached plugin");
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#put(java.lang.String, java.util.List)
     */
    public synchronized List<Object> put(String queryKey, List<Object> results)
    {
        if (queryKey == null || results == null)
        {
            return null;
        }

        client.set(keyPrefix + queryKey, expireSeconds, results);

        return results;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#size()
     */
    public int size()
    {
        throw new UnsupportedOperationException("size() method not supported by spymemcached plugin");
    }
}