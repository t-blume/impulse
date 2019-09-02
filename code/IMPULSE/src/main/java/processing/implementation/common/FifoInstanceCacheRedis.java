package main.java.processing.implementation.common;

import main.java.common.interfaces.ILocatable;
import main.java.common.interfaces.IResource;
import main.java.processing.interfaces.IElementCache;
import main.java.processing.interfaces.IElementCacheListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * A first-in-first-out (FIFO) cache implementation for locatable Objects.
 * Objects are stored as long as the internal capacity is not exhausted. At that
 * point, the earliest added entry will be removed to clear some space for the
 * new entry. If the cache is closed, all not yet removed entries will be
 * removed and listeners will be informed of the closing. Each removed entry
 * will be given to the registered listeners
 *
 * @param <T> The instanceelements type
 * @author Bastian
 */
public class FifoInstanceCacheRedis<T extends ILocatable> implements
        IElementCache<T> {
    private static final Logger logger = LogManager.getLogger(FifoInstanceCacheRedis.class.getSimpleName());

    private int deletionCounter = 0;

    private RedissonClient redisson;
    private RMap<IResource, T> redissoMap;
    private Queue<IResource> queue;
    private int capacity;
    private List<IElementCacheListener<T>> listeners;

    // FIXME int might be too small, Queue only allows int as size
    private int maxSize = 0;

    /**
     * Constructor. Creates a cache with the highest integer as capacity
     */
    public FifoInstanceCacheRedis() {
        this(Integer.MAX_VALUE);
    }

    /**
     * Constructor. Creates a cache with the given capacity
     *
     * @param capacity The capacity
     */
    public FifoInstanceCacheRedis(int capacity) {
        redisson = Redisson.create();
        redissoMap = redisson.getMap("instances");
        listeners = new ArrayList<>();
        queue = new ArrayDeque<>();
        this.capacity = capacity;
    }

    @Override
    public boolean contains(T i) {
        boolean contains = redissoMap.containsKey(i.getLocator());
        System.out.println(contains);
        return contains;
       // return redisson.getBucket(i.getLocator().toString()).get() != null;
    }

    @Override
    public boolean contains(IResource res) {
        boolean contains = redissoMap.containsKey(res);
        System.out.println(contains);
        return contains;
//        return redisson.getBucket(res.toString()).get() != null;
    }

    @Override
    public T get(IResource res) {
        return redissoMap.get(res);
       // return (T) redisson.getBucket(res.toString()).get();
    }

    @Override
    public int size() {
        return queue.size(); //use queue for size instead of elements (Goldstandard edit @Till)
    }

    @Override
    public void add(T i) {
        if (capacity <= 0)//deactivated cache
            return;
        if (!contains(i)) {
            int currentSize = size();
            maxSize = Math.max(maxSize, currentSize);
            if (capacity == Integer.MAX_VALUE && currentSize % 1000 == 0)
                System.out.format("\t\t\t\t\t\t\t\t\t\t\t\t\t\tIC: %08d / %08d\r", currentSize, maxSize);

            // If the capacity is full -> remove first entry added
            if (currentSize == capacity) {
                if (capacity == Integer.MAX_VALUE) {
                    System.out.format("\nInstance Cache limit %08d reached - Gold standard failed !\n", maxSize);
                    System.exit(-1);
                }
                removeLast();
            }
            redissoMap.put(i.getLocator(), i);
            queue.add(i.getLocator());
        }
    }

    private void removeLast() {
        IResource first = queue.poll();
        T el = get(first);
        int currentSize = size();
        maxSize = Math.max(maxSize, currentSize);
        if (capacity == Integer.MAX_VALUE && maxSize % 1000 == 0)
            System.out.format("\t\t\t\t\t\t\t\t\t\t\t\t\t\tIC: %08d / %08d\r", currentSize, maxSize);

        notifyListeners(el);
    }

    private void notifyListeners(T el) {
        for (IElementCacheListener<T> l : listeners)
            l.elementFlushed(el);

    }

    @Override
    public void flush(boolean deleteAfterwards) {
        while (size() != 0) {
            System.out.format("Items Left: %08d    \r", size());
            removeLast();
        }
        if (deleteAfterwards)
            queue = new ArrayDeque<>();

    }

    @Override
    public void registerCacheListener(IElementCacheListener<T> listener) {
        listeners.add(listener);
    }

    @Override
    public void close() {
        logger.info("Closing Instance Cache, flushing to listeners...");
        flush(true);
        for (IElementCacheListener<T> l : listeners)
            l.finished();
    }

}
