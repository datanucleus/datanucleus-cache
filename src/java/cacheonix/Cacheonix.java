package cacheonix;

import cacheonix.cache.Cache;

public abstract class Cacheonix
{
    public static Cacheonix getInstance()
    {
        return null;
    }

    public static Cacheonix getInstance(String filename)
    {
        return null;
    }

    public void shutdown()
    {
    }

    public abstract boolean cacheExists(String name);
    public abstract Cache createCache(String name);
    public abstract <K extends java.io.Serializable,V extends java.io.Serializable> Cache<K,V> getCache(String cacheName);
}
