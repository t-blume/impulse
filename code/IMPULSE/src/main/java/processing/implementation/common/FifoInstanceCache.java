package main.java.processing.implementation.common;

import main.java.common.interfaces.ILocatable;
import main.java.common.interfaces.IResource;
import main.java.processing.interfaces.IElementCache;
import main.java.processing.interfaces.IElementCacheListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

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
public class FifoInstanceCache<T extends ILocatable> implements
        IElementCache<T> {
    private static final Logger logger = LogManager.getLogger(FifoInstanceCache.class.getSimpleName());

    private int deletionCounter = 0;

    private Map<IResource, T> elements;
    private Queue<IResource> queue;
    private int capacity;
    private List<IElementCacheListener<T>> listeners;

    // FIXME int might be too small, Queue only allows int as size
    private int maxSize = 0;

    /**
     * Constructor. Creates a cache with the highest integer as capacity
     */
    public FifoInstanceCache() {
        this(Integer.MAX_VALUE);
    }

    /**
     * Constructor. Creates a cache with the given capacity
     *
     * @param capacity The capacity
     */
    public FifoInstanceCache(int capacity) {
        elements = new HashMap<>();
        listeners = new ArrayList<>();
        queue = new ArrayDeque<>();
        this.capacity = capacity;
    }

    @Override
    public boolean contains(T i) {
        return elements.containsKey(i.getLocator());
    }

    @Override
    public boolean contains(IResource res) {
        return elements.containsKey(res);
    }

    @Override
    public T get(IResource res) {
        return elements.get(res);
    }

    public List<String> get2() {
        List<String> list = new ArrayList<>();
        for (Map.Entry<IResource, T> entry : elements.entrySet()) {
            list.add(entry.getKey() + "/" + entry.getValue());
        }
        return list;
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

            elements.put(i.getLocator(), i);
            queue.add(i.getLocator());
        }

    }

    private void removeLast() {
        IResource first = queue.poll();
        T el = get(first);
        if (capacity != Integer.MAX_VALUE) { //do not remove flushed instance when constructing gold standard
            elements.remove(first);
            deletionCounter++;
        }
        int currentSize = size();
        maxSize = Math.max(maxSize, currentSize);
        if (capacity == Integer.MAX_VALUE && maxSize % 1000 == 0)
            System.out.format("\t\t\t\t\t\t\t\t\t\t\t\t\t\tIC: %08d / %08d\r", currentSize, maxSize);

        if (10 * capacity > 0 && deletionCounter > 10 * capacity) { //integer overflow
            long start = System.currentTimeMillis();
            Date date = new Date();
            System.out.format("%tc: Cleaning cache....\n", date);
            Map<IResource, T> newElements = new HashMap<>();
            for (Map.Entry<IResource, T> entry : elements.entrySet())
                newElements.put(entry.getKey(), entry.getValue());

            elements = newElements;
            date = new Date();
            System.out.format("%tc: ... done cleaning in %08d ms\n", date, System.currentTimeMillis() - start);
            deletionCounter = 0;
        }

        notifyListeners(el);
    }

    private void notifyListeners(T el) {
        for (IElementCacheListener<T> l : listeners) {
            l.elementFlushed(el);
        }
    }

    @Override
    public void flush(boolean deleteAfterwards) {
        while (size() != 0) {
            System.out.format("Items Left: %08d    \r", size());
            removeLast();
        }
        if(deleteAfterwards){
            elements = new HashMap<>();
            queue = new ArrayDeque<>();
        }
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
