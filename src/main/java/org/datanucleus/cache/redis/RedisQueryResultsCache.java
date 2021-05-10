/**********************************************************************
Copyright (c) 2017 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.cache.redis;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.datanucleus.Configuration;
import org.datanucleus.NucleusContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.query.QueryUtils;
import org.datanucleus.store.query.cache.AbstractQueryResultsCache;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.util.Pool;

/**
 * Plugin using <a href="https://redis.io/">Redis</a> as a QueryResults cache.
 * Dependent on Jedis, and Apache Commons Pool2.
 */
public class RedisQueryResultsCache extends AbstractQueryResultsCache
{
    private static final long serialVersionUID = 491530711712812608L;

    public static final String PROPERTY_CACHE_QUERYRESULTS_REDIS_DATABASE = "datanucleus.cache.queryResults.redis.database";
    public static final String PROPERTY_CACHE_QUERYRESULTS_REDIS_TIMEOUT = "datanucleus.cache.queryResults.redis.timeout";
    public static final String PROPERTY_CACHE_QUERYRESULTS_REDIS_SENTINELS = "datanucleus.cache.queryResults.redis.sentinels";
    public static final String PROPERTY_CACHE_QUERYRESULTS_REDIS_SERVER = "datanucleus.cache.queryResults.redis.server";
    public static final String PROPERTY_CACHE_QUERYRESULTS_REDIS_PORT = "datanucleus.cache.queryResults.redis.port";

    Pool<Jedis> pool;

    int expirySeconds;

    private final static String DEFAULT_SERVER = "localhost";
    private final static int DEFAULT_DATABASE = 1;
    private final static int DEFAULT_PORT = 6379;
    private final static int DEFAULT_TIMEOUT = 5000;

    /** Prefix (for uniqueness) */
    private static final String KEY_PREFIX = "datanucleus-query:";

    public RedisQueryResultsCache(NucleusContext nucleusCtx)
    {
        super(nucleusCtx);

        Configuration conf = nucleusCtx.getConfiguration();

        int database = conf.getIntProperty(PROPERTY_CACHE_QUERYRESULTS_REDIS_DATABASE);
        database = database == 0 ? DEFAULT_DATABASE : database;

        expirySeconds = (int) (expiryMillis/1000);

        int timeout = conf.getIntProperty(PROPERTY_CACHE_QUERYRESULTS_REDIS_TIMEOUT);
        timeout = timeout == 0 ? DEFAULT_TIMEOUT : timeout;

        String sentinelsStr = conf.getStringProperty(PROPERTY_CACHE_QUERYRESULTS_REDIS_SENTINELS);
        if (sentinelsStr != null && sentinelsStr.length() > 0)
        {
            Set<String> sentinels = new LinkedHashSet<>();
            sentinels.addAll(Arrays.asList(sentinelsStr.split(",")));
            pool = new JedisSentinelPool("mymaster", sentinels, new GenericObjectPoolConfig(), timeout, null, database);
        }
        else
        {
            String server = conf.getStringProperty(PROPERTY_CACHE_QUERYRESULTS_REDIS_SERVER);
            int port = conf.getIntProperty(PROPERTY_CACHE_QUERYRESULTS_REDIS_PORT);

            server = server == null ? DEFAULT_SERVER : server;
            port = port == 0 ? DEFAULT_PORT : port;

            pool = new JedisPool(new JedisPoolConfig(), server, port, timeout, null, database);
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#close()
     */
    public void close()
    {
        if (clearAtClose)
        {
            try
            {
                Jedis jedis = pool.getResource();
                jedis.flushDB();
                jedis.close();
                pool.close();
            }
            catch (Exception e)
            {
                throw new NucleusException("Could not close connection to Redis cache", e);
            }
        }
        else
        {
            try
            {
                pool.close();
            }
            catch (Exception e)
            {
                throw new NucleusException("Could not close connection to Redis cache", e);
            }
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#evict(java.lang.Class)
     */
    public void evict(Class candidate)
    {
        /* TODO */
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#evict(org.datanucleus.store.query.Query)
     */
    public void evict(Query query)
    {
        Jedis jedis = null;
        String key = null;
        try
        {
            jedis = pool.getResource();
            key = QueryUtils.getKeyForQueryResultsCache(query, null);
            jedis.del(key);
            pool.returnResource(jedis);
        }
        catch (Exception e)
        {
            pool.returnBrokenResource(jedis);
            throw new NucleusException(String.format("Failed to evict key %s from Redis cache", key), e);
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#evict(org.datanucleus.store.query.Query, java.util.Map)
     */
    public void evict(Query query, Map params)
    {
        Jedis jedis = null;
        String key = null;
        try
        {
            jedis = pool.getResource();
            key = QueryUtils.getKeyForQueryResultsCache(query, params);
            jedis.del(key);
            pool.returnResource(jedis);
        }
        catch (Exception e)
        {
            pool.returnBrokenResource(jedis);
            throw new NucleusException(String.format("Failed to evict key %s from Redis cache", key), e);
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#evictAll()
     */
    public void evictAll()
    {
        Jedis jedis = null;
        try
        {
            jedis = pool.getResource();
            jedis.flushDB();
            pool.returnResource(jedis);
        }
        catch (Exception e)
        {
            pool.returnBrokenResource(jedis);
            throw new NucleusException("Failed to evict-all from Redis cache", e);
        }

    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#get(java.lang.String)
     */
    public List<Object> get(String queryKey)
    {
        String key = KEY_PREFIX + queryKey;
        Jedis jedis = null;
        Object value;
        try
        {
            jedis = pool.getResource();
            value = getObjectFromBytes(jedis.get(key.getBytes()));
            pool.returnResource(jedis);
        }
        catch (Exception e)
        {
            pool.returnBrokenResource(jedis);
            throw new NucleusException("Failed to get from Redis cache", e);
        }

        return (List<Object>) value;
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

        Jedis jedis = null;
        String key = KEY_PREFIX + queryKey;
        try
        {
            jedis = pool.getResource();
            jedis.setex(key.getBytes(), expirySeconds, getBytesForObject(results));
            pool.returnResource(jedis);
        }
        catch (Exception e)
        {
            pool.returnBrokenResource(jedis);
            throw new NucleusException(String.format("Failed to set object %s with ID %s into Redis cache", results, queryKey));
        }

        return results;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.cache.QueryResultsCache#contains(java.lang.String)
     */
    public boolean contains(String queryKey)
    {
        return get(KEY_PREFIX + queryKey) != null;
    }

    private Object getObjectFromBytes(byte[] bytes)
    {
        Object o = null;
        if (bytes != null)
        {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInput in = null;

            try
            {
                in = new ObjectInputStream(bis);
                o = in.readObject();
            }
            catch (ClassNotFoundException | IOException e)
            {
                throw new NucleusException("Failed to convert object", e);
            }
            finally
            {
                try
                {
                    bis.close();
                }
                catch (IOException ex)
                {
                    // ignore close exception
                }
                try
                {
                    if (in != null)
                    {
                        in.close();
                    }
                }
                catch (IOException ex)
                {
                    // ignore close exception
                }
            }
        }
        return o;
    }

    private byte[] getBytesForObject(Object obj)
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try
        {
            ObjectOutputStream objStream = new ObjectOutputStream(bos);
            objStream.writeObject(obj);
        }
        catch (IOException e)
        {
            throw new NucleusException("Exception in serializing Object for Redis cache", e);
        }

        return bos.toByteArray();
    }
}