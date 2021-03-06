/**********************************************************************
Copyright (c) 2009 Clive Cox and others. All rights reserved.
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
2013 Andy Jefferson - cater for hashing of ids
    ...
**********************************************************************/
package org.datanucleus.cache.spymemcached;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;

import org.datanucleus.NucleusContext;
import org.datanucleus.Configuration;
import org.datanucleus.cache.AbstractLevel2Cache;
import org.datanucleus.cache.CachedPC;
import org.datanucleus.cache.xmemcached.XmemcachedLevel2Cache;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.identity.SingleFieldId;
import org.datanucleus.util.NucleusLogger;

/**
 * Plugin using Spymemcached implementation of "memcached" as a Level2 cache.
 * We use the "cacheName" as the keyPrefix to distinguish our objects from others in memcached.
 */
public class SpymemcachedLevel2Cache extends AbstractLevel2Cache
{
    private static final long serialVersionUID = 6424542848352545662L;

    private MemcachedClient client;

    private int expireSeconds = 0;

    public SpymemcachedLevel2Cache(NucleusContext nucleusCtx)
    {
        super(nucleusCtx);

        Configuration conf = nucleusCtx.getConfiguration();

        expireSeconds = (int)expiryMillis/1000;

        try
        {
            String servers = conf.getStringProperty(XmemcachedLevel2Cache.PROPERTY_CACHE_L2_MEMCACHED_SERVERS);
            client = new MemcachedClient(AddrUtil.getAddresses(servers));
        }
        catch (IOException e)
        {
            NucleusLogger.CACHE.error("Exception caught creating cache", e);
            throw new NucleusException("Cant create cache", e);
        }
    }

    public void close()
    {
        if (clearAtClose)
        {
            client.flush();
            client.shutdown();
        }
    }

    public boolean containsOid(Object oid)
    {
        return get(oid) != null;
    }

    public void evict(Object oid)
    {
        client.delete(getCacheKeyForId(oid));
    }

    public void evictAll()
    {
        client.flush();
    }

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

    public void evictAll(Class arg0, boolean arg1)
    {
        // Not supported. Do nothing
    }

    public CachedPC get(Object oid)
    {
        return (CachedPC) client.get(getCacheKeyForId(oid));
    }

    public CachedPC put(Object oid, CachedPC pc)
    {
        if (oid == null || pc == null)
        {
            return null;
        }

        client.set(getCacheKeyForId(oid), expireSeconds, pc);
        return pc;
    }

    protected String getCacheKeyForId(Object id)
    {
        // Because single-field id doesn't include target class name in toString()
        if (IdentityUtils.isSingleFieldIdentity(id))
        {
            String targetClassName = ((SingleFieldId)id).getTargetClassName();
            return cacheName + targetClassName + ":" + id.toString().hashCode();
        }
        return cacheName + id.toString().hashCode();
    }
}