package main.java.processing.implementation.common;

import main.java.common.interfaces.ILocatable;
import main.java.common.interfaces.IResource;
import main.java.processing.interfaces.IElementCache;
import main.java.processing.interfaces.IElementCacheListener;
import main.java.utils.LRUCache;
import org.apache.commons.collections.ArrayStack;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.*;


/**
 * @param <T>
 */
public class LRUFiFoInstanceCache<T extends ILocatable> implements
        IElementCache<T> {
    private static final Logger logger = LogManager.getLogger(LRUFiFoInstanceCache.class.getSimpleName());
    private static final int loggingInterval = 5000;

    //location where to store elements that do no fit in main memory
    private String diskCachedElements;
    //Least-Recently Used (LRU) elements stored in memory
    private LRUCache<IResource, T> memoryCachedElements;
    //keep track of all elements to evict them later: First-in-First-out (FiFo).
    //allow more than Integer.MAX_VALUE elements, i.e., Integer.MAX_VALUE * Integer.MAX_VALUE values
    private Stack<Queue<IResource>> fifoQueues;

    //maximum number of elements store in memory
    private int capacity;

    //maximum number of elements actually stored in memory/disk
    private long maxSize = 0;

    //flush elements to each listener afterwards
    private List<IElementCacheListener<T>> listeners;

    /**
     * Constructor. Creates a cache with the highest integer as capacity
     */
    public LRUFiFoInstanceCache() {
        this(Integer.MAX_VALUE, "tmp");
    }

    public LRUFiFoInstanceCache(int capacity) {
        this(capacity, "disk-cache");
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
        fifoQueues = new Stack<>();
        fifoQueues.add(new ArrayDeque<>());
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
                put(element);
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
    public long size() {
        long size = 0L;
        for (Queue<IResource> queue : fifoQueues)
            size += queue.size();
        return size;
    }


    private void put(T i) {
        // if memory is full -> move eldest entry to disk
        if (memoryCachedElements.size() >= capacity) {
            T eldestElement = memoryCachedElements.getEldestEntry().getValue();
            if (size() % loggingInterval == 0)
                System.out.format("Instances parsed: %08d    \r", size());

            //do not delete old element but store on disk
            saveToDisk(eldestElement);
            memoryCachedElements.remove(eldestElement.getLocator());
        }
        //finally, add new element to cache
        memoryCachedElements.put(i.getLocator(), i);
    }


    /**
     * ArrayDeque.MAX_ARRAY_SIZE = 2147483639
     *
     * @param resource
     */
    private void addToQueue(IResource resource) {
        //start from last queue
        if (fifoQueues.get(fifoQueues.size() - 1).size() >= 2147483639) {
            //last queue is already full
            ArrayDeque<IResource> newQueue = new ArrayDeque<>();
            newQueue.add(resource);
            fifoQueues.push(newQueue);
        } else
            fifoQueues.get(fifoQueues.size() - 1).add(resource);
    }

    private IResource pollFromQueue() {
        //start from last queue
        if (fifoQueues.peek().size() <= 0) {
            //last queue is empty, remove it
            fifoQueues.pop();
        }
        //if there is still a queue, return an element from it
        if(fifoQueues.size() > 0)
            return fifoQueues.peek().poll();
        else
            return null;
    }

    @Override
    public void add(T i) {
        if (!contains(i)) {
            //new resource, add to queue
            addToQueue(i.getLocator());
            //increase total counter
            maxSize++;
        }
        //add element to memory, dump something to disk if necessary
        put(i);
    }

    private void removeLast() {
        IResource first = pollFromQueue();
        T el = get(first);
        if (size() % loggingInterval == 0)
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
            if (size() % loggingInterval == 0)
                System.out.format("Items remaining: %08d    \r", size());
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
            fifoQueues = new Stack<>();
        }
    }

    @Override
    public void registerCacheListener(IElementCacheListener<T> listener) {
        listeners.add(listener);
    }

    @Override
    public void close() {
        logger.info("Closing Instance Cache, flushing " + size() + " instances to listeners.");
        //FIXME deleting more than MAX_INT files probably causes program to crash
        flush(false);
        for (IElementCacheListener<T> l : listeners)
            l.finished();
    }


    private boolean diskContains(IResource resource) {
        return new File(diskCachedElements + File.separator + resource.hashCode() + ".ser.tmp").exists();
    }

    private T getFromDisk(IResource resource) {
        T res = null;
        File file = new File(diskCachedElements + File.separator + resource.hashCode() + ".ser.tmp");
        if (file.exists()) {
            try {
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
                res = (T) ois.readObject();
                ois.close();
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
            oos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
