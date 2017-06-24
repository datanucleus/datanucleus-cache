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
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.datanucleus.Configuration;
import org.datanucleus.NucleusContext;
import org.datanucleus.cache.AbstractLevel2Cache;
import org.datanucleus.cache.CachedPC;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.identity.SingleFieldId;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.util.Pool;

/**
 * Plugin using <a href="https://redis.io/">Redis</a> as a Level2 cache.
 * Dependent on Jedis, and Apache Commons Pool2.
 */
public class RedisLevel2Cache extends AbstractLevel2Cache
{
    private static final long serialVersionUID = 4428364640009394044L;

    private Pool<Jedis> pool;

    private int expirySeconds;

    private final static String DEFAULT_SERVER = "localhost";
    private final static int DEFAULT_DATABASE = 1;
    private final static int DEFAULT_PORT = 6379;
    private final static int DEFAULT_TIMEOUT = 5000;
    private final static int DEFAULT_EXPIRY = 86400;

    public RedisLevel2Cache(NucleusContext nucleusCtx)
    {
        super(nucleusCtx);

        Configuration conf = nucleusCtx.getConfiguration();

        int database = conf.getIntProperty("datanucleus.cache.level2.redis.database");
        database = database == 0 ? DEFAULT_DATABASE : database;

        int timeout = conf.getIntProperty("datanucleus.cache.level2.redis.timeout");
        timeout = timeout == 0 ? DEFAULT_TIMEOUT : timeout;

        expirySeconds = conf.getIntProperty("datanucleus.cache.level2.redis.expirySeconds");
        expirySeconds = expirySeconds == 0 ? DEFAULT_EXPIRY : expirySeconds;

        String sentinelsStr = conf.getStringProperty("datanucleus.cache.level2.redis.sentinels");
        if (sentinelsStr != null && sentinelsStr.length() > 0)
        {
            Set<String> sentinels = new LinkedHashSet<>();
            sentinels.addAll(Arrays.asList(sentinelsStr.split(",")));
            pool = new JedisSentinelPool("mymaster", sentinels, new GenericObjectPoolConfig(), timeout, null, database);
        }
        else
        {
            String server = conf.getStringProperty("datanucleus.cache.level2.redis.server");
            int port = conf.getIntProperty("datanucleus.cache.level2.redis.port");

            server = server == null ? DEFAULT_SERVER : server;
            port = port == 0 ? DEFAULT_PORT : port;

            pool = new JedisPool(new JedisPoolConfig(), server, port, timeout, null, database);
        }
    }

    @Override
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
                throw new NucleusException("Error closing connection to Redis cache", e);
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
                throw new NucleusException("Error closing connection to Redis cache", e);
            }
        }
    }

    @Override
    public void evict(Object oid)
    {
        Jedis jedis = null;
        try
        {
            jedis = pool.getResource();
            jedis.del(getCacheKeyForId(oid).getBytes());
            pool.returnResource(jedis);
        }
        catch (Exception e)
        {
            pool.returnBrokenResource(jedis);
            throw new NucleusException(String.format("Failed to evict key %s from Redis cache", oid), e);
        }
    }

    @Override
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

    @Override
    public void evictAll(Object[] objects)
    {
        Jedis jedis = null;
        jedis = pool.getResource();
        for (Object oid : objects)
        {
            try
            {
                jedis.del(getCacheKeyForId(oid).getBytes());

            }
            catch (Exception e)
            {
                pool.returnBrokenResource(jedis);
                throw new NucleusException(String.format("Failed to evict keys %s from cache {0}", objects), e);
            }
        }
        pool.returnResource(jedis);
    }

    @Override
    public void evictAll(Collection collection)
    {
        evictAll(collection.toArray());
    }

    @Override
    public void evictAll(Class aClass, boolean b)
    {
        // Not supported. Do nothing
    }

    @Override
    public CachedPC get(Object oid)
    {
        Jedis jedis = null;
        Object value;
        try
        {
            jedis = pool.getResource();
            value = getObjectInternal(jedis.get(getCacheKeyForId(oid).getBytes()));
            pool.returnResource(jedis);
        }
        catch (Exception e)
        {
            pool.returnBrokenResource(jedis);
            throw new NucleusException(String.format("Failed to get key %s from cache {0}", oid), e);
        }
        return (CachedPC) value;
    }

    private Object getObjectInternal(byte[] bytes)
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

    @Override
    public CachedPC put(Object o, CachedPC cachedPC)
    {
        if (o == null || cachedPC == null)
        {
            return null;
        }

        Jedis jedis = null;
        try
        {
            jedis = pool.getResource();
            jedis.setex(getCacheKeyForId(o).getBytes(), expirySeconds, getBytesForObject(cachedPC));
            pool.returnResource(jedis);
        }
        catch (Exception e)
        {
            pool.returnBrokenResource(jedis);
            throw new NucleusException(String.format("Failed to set object %s with id %s into Redis cache", cachedPC, o));
        }

        return cachedPC;

    }

    @Override
    public boolean containsOid(Object o)
    {
        return get(o) != null;
    }

    protected byte[] getBytesForObject(Object obj) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream outputStream = new ObjectOutputStream(bos);
        outputStream.writeObject(obj);
        return bos.toByteArray();
    }

    protected String getCacheKeyForId(Object id)
    {
        if (IdentityUtils.isSingleFieldIdentity(id))
        {
            String targetClassName = ((SingleFieldId) id).getTargetClassName();
            return cacheName + targetClassName + ":" + id.toString().hashCode();
        }
        return cacheName + id.toString().hashCode();
    }
}