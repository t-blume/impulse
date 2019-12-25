package main.java.processing.implementation.common;

import main.java.common.interfaces.ILocatable;
import main.java.common.interfaces.IResource;
import main.java.processing.interfaces.IElementCache;
import main.java.processing.interfaces.IElementCacheListener;
import main.java.utils.LRUCache;
import main.java.utils.LongQueue;
import main.java.utils.MemoryTracker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static main.java.utils.MainUtils.deleteDirectory;


/**
 * @param <T>
 */
public class LRUFiFoInstanceCache<T extends ILocatable> implements
        IElementCache<T> {
    private static final Logger logger = LogManager.getLogger(LRUFiFoInstanceCache.class.getSimpleName());
    private static final int loggingInterval = 5000;

    private static final int DISK_CACHE_FOLDER_DEPTH = 2;
    //location where to store elements that do no fit in main memory
    private String diskCachedElements;
    //Least-Recently Used (LRU) elements stored in memory
    private LRUCache<IResource, T> memoryCachedElements;
    //keep track of all elements to evict them later: First-in-First-out (FiFo).
    //allow more than Integer.MAX_VALUE elements, i.e., Integer.MAX_VALUE * Integer.MAX_VALUE values
    private LongQueue<IResource> fifoQueue;

    //maximum number of elements store in memory
    private int capacity;

    //maximum number of elements actually stored in memory/disk
    private long maxSize = 0;
    private MemoryTracker memoryTracker = new MemoryTracker("stats2");

    //flush elements to each listener afterwards
    private List<IElementCacheListener> listeners;

    //delete disk cache afterwards?
    private boolean deleteDiskCache;


    private Object sync = new Object();


    private void initSubFolders(String baseFolder, int depth) {
        for (int i = -9; i < 10; i++) {
            new File(baseFolder + File.separator + i).mkdir();
            if (depth < DISK_CACHE_FOLDER_DEPTH)
                initSubFolders(baseFolder + File.separator + i, depth + 1);
        }
    }

    /**
     * Constructor. Creates a cache with the given capacity
     *
     * @param capacity The capacity
     */
    public LRUFiFoInstanceCache(int capacity, String diskCachedElements, boolean deleteDiskCache) {
        this.diskCachedElements = diskCachedElements;
        new File(diskCachedElements).mkdirs();
        initSubFolders(diskCachedElements, 1);

        memoryCachedElements = new LRUCache<>(capacity, 0.75f);
        listeners = new ArrayList<>();
        fifoQueue = new LongQueue<>();
        this.capacity = capacity;
        this.deleteDiskCache = deleteDiskCache;
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
                synchronized (sync) {
                    T element = getFromDisk(res);
                    //add to cache
                    put(element);
                    return element;
                }
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
        return fifoQueue.size();
    }

    private void removeEldest() {
        T eldestElement = memoryCachedElements.getEldestEntry().getValue();
        if (size() % loggingInterval == 0)
            System.out.format("Instances parsed: %08d    \r", size());

        //do not delete old element but store on disk
        saveToDisk(eldestElement);
        memoryCachedElements.remove(eldestElement.getLocator());
    }

    private void put(T i) {
        // if memory is full -> move eldest entry to disk
        synchronized (sync) {
            try {
                if (memoryCachedElements.size() >= capacity) {
                    removeEldest();
                }
                //finally, add new element to cache
                memoryCachedElements.put(i.getLocator(), i);
            } catch (OutOfMemoryError e) {
                //TODO: handle memory leak
                for (int j=0; j < 1000; j++)
                    removeEldest();

                this.capacity = memoryCachedElements.size();
                memoryCachedElements.put(i.getLocator(), i);
            }
        }
    }


    /**
     * ArrayDeque.MAX_ARRAY_SIZE = 2147483639
     *
     * @param resource
     */
    private void addToQueue(IResource resource) {
        synchronized (sync) {
            fifoQueue.add(resource);
        }
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

//    private void removeLast() {
//        IResource first = pollFromQueue();
//        T el = get(first);
//        if (size() % loggingInterval == 0)
//            System.out.format("\t\t\t\t\t\t\t\t\t\t\t\t\t\tIC: %08d / %08d\r", size(), maxSize);
//
//        notifyListeners(el);
//    }

//    private void notifyListeners(T el) {
//        for (IElementCacheListener l : listeners)
//            l.elementFlushed(el);
//
//    }

    @Override
    public void flush() {
//        while (size() != 0) {
//            if (size() % loggingInterval == 0)
//                System.out.format("Items remaining: %08d    \r", size());
//            removeLast();
//        }
        memoryCachedElements = new LRUCache<>(capacity, 0.75f);
        fifoQueue = new LongQueue<>();
        if (deleteDiskCache)
            deleteDirectory(diskCachedElements);
    }

    @Override
    public void registerCacheListener(IElementCacheListener listener) {
        listeners.add(listener);
    }

    @Override
    public void close() {
        logger.info("Closing Instance Cache, flushing " + size() + " instances to listeners.");
        for (IElementCacheListener l : listeners)
            l.startWorking(fifoQueue.clone());

        for (IElementCacheListener l : listeners) {
            try {
                l.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    private boolean diskContains(IResource resource) {
        return new File(hashToFilename(resource.hashCode())).exists();
    }

    private T getFromDisk(IResource resource) {
        T res = null;
        File file = new File(hashToFilename(resource.hashCode()));
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
        File file = new File(hashToFilename(element.getLocator().hashCode()));
        try {
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file));
            oos.writeObject(element);
            oos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private String hashToFilename(int hashcode) {
        String filename = diskCachedElements + File.separator;
        String tmp = String.valueOf(hashcode);
        if (tmp.startsWith("-")) {
            filename += '-';
            tmp = tmp.replaceFirst("-", "");
        }
        char[] hashDigits = tmp.toCharArray();
        for (int i = 0; i < DISK_CACHE_FOLDER_DEPTH; i++)
            filename += hashDigits[i] + File.separator;


        return filename + hashcode + ".ser.tmp";
    }
}
