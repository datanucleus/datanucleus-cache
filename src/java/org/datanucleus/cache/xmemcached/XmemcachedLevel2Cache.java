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
package org.datanucleus.cache.xmemcached;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.utils.AddrUtil;

import org.datanucleus.NucleusContext;
import org.datanucleus.Configuration;
import org.datanucleus.cache.AbstractLevel2Cache;
import org.datanucleus.cache.CachedPC;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.identity.SingleFieldId;
import org.datanucleus.util.StringUtils;

/**
 * Plugin using Xmemcached implementation of "memcached" as a Level2 cache.
 */
public class XmemcachedLevel2Cache extends AbstractLevel2Cache
{
    private MemcachedClient client;

    private int expireSeconds = 0;

    public XmemcachedLevel2Cache(NucleusContext nucleusCtx)
    {
        super(nucleusCtx);

        Configuration conf = nucleusCtx.getConfiguration();

        String servers = conf.getStringProperty("datanucleus.cache.level2.memcached.servers");
        MemcachedClientBuilder builder = new XMemcachedClientBuilder(AddrUtil.getAddresses(servers));
        try
        {
            client = builder.build();
        }
        catch (IOException e)
        {
            e.printStackTrace();
            throw new NucleusException("Cant create cache", e);
        }

        String expireStr = conf.getStringProperty("datanucleus.cache.level2.memcached.expireSeconds");
        if (!StringUtils.isWhitespace(expireStr))
        {
            expireSeconds = Integer.parseInt(expireStr);
        }
    }

    public void close()
    {
        if (clearAtClose)
        {
            try
            {
                client.flushAll();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
        try
        {
            client.shutdown();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public boolean containsOid(Object oid)
    {
        return get(oid) != null;
    }

    public synchronized void evict(Object oid)
    {
        try
        {
            client.delete(getCacheKeyForId(oid));
        }
        catch (Exception e)
        {
            throw new NucleusException("Exception evict entry from xmemcached", e);
        }
    }

    public synchronized void evictAll()
    {
        try
        {
            client.flushAll();
        }
        catch (Exception e)
        {
            throw new NucleusException("Exception evict entries from xmemcached", e);
        }
    }

    public synchronized void evictAll(Object[] oids)
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

    public synchronized void evictAll(Collection oids)
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

    public synchronized void evictAll(Class arg0, boolean arg1)
    {
        throw new UnsupportedOperationException("evictAll(Class,Boolean) method not supported by xmemcached plugin");
    }

    public CachedPC get(Object oid)
    {
        try
        {
            return (CachedPC) client.get(getCacheKeyForId(oid));
        }
        catch (Exception e)
        {
            throw new NucleusException("Exception thrown in retrieval from xmemcached", e);
        }
    }

    public int getSize()
    {
        throw new UnsupportedOperationException("getSize() method not supported by xmemcached plugin");
    }

    public boolean isEmpty()
    {
        throw new UnsupportedOperationException("isEmpty() method not supported by xmemcached plugin");
    }

    public synchronized CachedPC put(Object oid, CachedPC pc)
    {
        if (oid == null || pc == null)
        {
            return null;
        }

        try
        {
            client.set(getCacheKeyForId(oid), expireSeconds, pc);
        }
        catch (Exception e)
        {
            throw new NucleusException("Exception thrown in persistence to xmemcached", e);
        }

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
        else
        {
            return cacheName + id.toString().hashCode();
        }
    }
}