package main.java.processing.implementation.common;

import main.java.common.interfaces.ILocatable;
import main.java.common.interfaces.IResource;
import main.java.processing.interfaces.IElementCache;
import main.java.processing.interfaces.IElementCacheListener;
import main.java.utils.LRUCache;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
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
public class LRUFiFoInstanceCache<T extends ILocatable> implements
        IElementCache<T> {
    private static final Logger logger = LogManager.getLogger(LRUFiFoInstanceCache.class.getSimpleName());

    private String diskCachedElements;
    private LRUCache<IResource, T> memoryCachedElements;
    private Queue<IResource> fifoQueue;
    private int capacity;
    private List<IElementCacheListener<T>> listeners;

    // FIXME int might be too small, Queue only allows int as size
    private int maxSize = 0;

    /**
     * Constructor. Creates a cache with the highest integer as capacity
     */
    public LRUFiFoInstanceCache() {
        this(Integer.MAX_VALUE, "tmp");
    }

    public LRUFiFoInstanceCache(int capacity) {
        this(capacity, "tmp");
    }

    /**
     * Constructor. Creates a cache with the given capacity
     *
     * @param capacity The capacity
     */
    public LRUFiFoInstanceCache(int capacity, String diskCachedElements) {
        this.diskCachedElements = diskCachedElements;
        new File(diskCachedElements).mkdirs();
        memoryCachedElements = new LRUCache<>(capacity, 0.75f);
        listeners = new ArrayList<>();
        fifoQueue = new ArrayDeque<>();
        this.capacity = capacity;
    }

    @Override
    public boolean contains(T i) {
        return contains(i.getLocator());
    }

    @Override
    public boolean contains(IResource res) {
        boolean contains = memoryCachedElements.containsKey(res);
        if (!contains)
            return diskContains(res);
        else
            return true;
    }

    @Override
    public T get(IResource res) {
        //element not in memory
        if (!memoryCachedElements.containsKey(res)) {
            //element not on disk
            if (!diskContains(res))
                return null;
            else {
                T element = getFromDisk(res);
                //add to cache
                //TODO
                memoryCachedElements.put(element.getLocator(), element);
                return element;
            }
        } else
            return memoryCachedElements.get(res);

    }


    /**
     * number of total elements in memory
     *
     * @return
     */
    @Override
    public int size() {
        return fifoQueue.size();
    }


    private void put(T i) {
        // if memory is full -> move eldest entry to disk
        if (memoryCachedElements.size() >= capacity) {
            T eldestElement = memoryCachedElements.getEldestEntry().getValue();
            logger.debug("Dumping instance to disk, memory cache size:  " + memoryCachedElements.size() + ", total: " + size());
            saveToDisk(eldestElement);
            memoryCachedElements.remove(eldestElement.getLocator());
        }
        //finally, add new element to cache
        memoryCachedElements.put(i.getLocator(), i);
    }

    @Override
    public void add(T i) {
        if (!contains(i)) {
            //completely new!!!
            fifoQueue.add(i.getLocator());
            //increase total counter
            maxSize = Math.max(maxSize, size());
            //add element to memory, dump something to disk if necessary
        }
        put(i);
        //else if in memory cache, simply overwrite
//        if(memoryCachedElements.containsKey(i.getLocator()))
//            memoryCachedElements.put(i.getLocator(), i);
//        else{
        //is currently stored on disk only
        //add element to memory, dump something to disk if necessary
        //           put(i);
//        }


    }

    private void removeLast() {
        IResource first = fifoQueue.poll();
        T el = get(first);
        if (size() % 1000 == 0)
            System.out.format("\t\t\t\t\t\t\t\t\t\t\t\t\t\tIC: %08d / %08d\r", size(), maxSize);

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
        if (deleteAfterwards) {
            memoryCachedElements = new LRUCache<>(capacity, 0.75f);
            File tmpFolder = new File(diskCachedElements);

            String[] entries = tmpFolder.list();
            for (String s : entries) {
                File currentFile = new File(tmpFolder.getPath(), s);
                currentFile.delete();
            }
            tmpFolder.delete();
            fifoQueue = new ArrayDeque<>();
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


    private boolean diskContains(IResource resource) {
        return new File(diskCachedElements + File.separator + String.valueOf(resource.hashCode()) + ".ser.tmp").exists();
    }

    private T getFromDisk(IResource resource) {
        T res = null;
        File file = new File(diskCachedElements + File.separator + String.valueOf(resource.hashCode()) + ".ser.tmp");
        if (file.exists()) {
            try {
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
                res = (T) ois.readObject();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        return res;
    }

    private void saveToDisk(T element) {
        File file = new File(diskCachedElements + File.separator + String.valueOf(element.getLocator().hashCode()) + ".ser.tmp");
        try {
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file));
            oos.writeObject(element);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
