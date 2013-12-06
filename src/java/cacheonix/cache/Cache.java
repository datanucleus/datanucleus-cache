package cacheonix.cache;

import java.util.Map;

public interface Cache<K extends java.io.Serializable,V extends java.io.Serializable> extends Map<K, V>
{
    V get(Object key);
    String getName();
    V put(K key, V value);
    V put(K key, V value, long expirationMillis);
    V remove(K key);
}
