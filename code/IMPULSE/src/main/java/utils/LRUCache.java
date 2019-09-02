package main.java.utils;


import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by bastian on 20.01.17.
 */
public class LRUCache<K, V> extends LinkedHashMap<K, V> {

    /**
     *
     */
    private static final long serialVersionUID = -1284425808657196428L;
    private Map.Entry<K, V> eldestEntry;

    public LRUCache(int capacity, float loadFactor) {

        // The accessOrder flag is really important, since this will
        // put the most used elements at the front and the
        // least used elements at the back of the map
        super(capacity, loadFactor, true);
    }

    /**
     * This method returns true if the eldest (least recently used)
     * entry should be evicted. Normally, that element would be deleted then.
     * Due to the SchemEx requirement that we need the lru element to write
     * it into the schema (SchemEx does this only at eviction time) we
     * cannot use this as implemented in Java. We will remove it by ourselves
     * and then return false to prevent the Java implementation to remove
     * it.
     * <br><br>
     * This usecase is specified in the Java Docs:
     * https://docs.oracle.com/javase/8/docs/api/java/util/LinkedHashMap.html#removeEldestEntry-java.util.Map.Entry-
     * <br><br>
     *
     * @param eldest The least recently used entry
     * @return false everytime. In case we would return true (eviction case)
     * we need to return false anyway and handle the eviction by ourselves.
     */
    @Override
    protected boolean removeEldestEntry(final Map.Entry<K, V> eldest) {
        eldestEntry = eldest;
        return false;
    }

    /**
     * To get the eldest entry. The eldest entry is updated after
     * element insertion only.
     * Do not use this method, if the previous interaction with the list was not a put().
     * The eldest entry is updated after put() only.
     * Use list.values().iterator().next() instead.
     * <br><br>
     *
     * @return The eldest element from this window.
     */
    public Map.Entry<K, V> getEldestEntry() {
        return eldestEntry;
    }
}
