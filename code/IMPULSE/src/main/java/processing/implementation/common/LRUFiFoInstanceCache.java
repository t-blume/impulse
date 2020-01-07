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
    private static final int loggingInterval = 1000000;
    private static final int memoryLoggingInterval = 10000000;

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
    private MemoryTracker memoryTracker = new MemoryTracker("stats2", memoryLoggingInterval);

    //flush elements to each listener afterwards
    private List<IElementCacheListener> listeners;

    //delete disk cache afterwards?
    private boolean deleteDiskCache;


    private Object sync = new Object();

    private long lastTime = System.currentTimeMillis();
    private long start = System.currentTimeMillis();

    private double maxTime = Double.MIN_VALUE;
    private double minTime = Double.MAX_VALUE;

    private long maxMemory = 0;
    private long maxUsedMemory = 0;

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
        //do not delete old element but store on disk
        saveToDisk(eldestElement);
        memoryCachedElements.remove(eldestElement.getLocator());
    }

    private void put(T i) {
        // if memory is full -> move eldest entry to disk
        synchronized (sync) {
            if (memoryCachedElements.size() >= capacity)
                removeEldest();

            //finally, add new element to cache
            memoryCachedElements.put(i.getLocator(), i);
            if(size() % loggingInterval == 0) {
                logger.info("--------------------------------------");
                logger.debug("Instances parsed: " + String.format("%,d", size()));
                logger.debug("Memory cached files: " + String.format("%,d", memoryCachedElements.size()));
                logger.debug("Queue Size: " + String.format("%,d", fifoQueue.size()));
                //System.out.format("Instances parsed: %08d    \r", size());
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
            if (maxSize % memoryLoggingInterval == 0) {
                long currentTime = System.currentTimeMillis();
                long delta = currentTime - lastTime;
                lastTime = currentTime;
                double instancesPerSecond = ((double) memoryLoggingInterval / delta * 1000.0);
                maxTime = Math.max(instancesPerSecond, maxTime);
                minTime = Math.min(instancesPerSecond, minTime);

                long runtimeMaxMemory = memoryTracker.getCurrentlyMaxMemory();
                long runtimeUsedMemory = memoryTracker.getReallyUsedMemory();

                maxMemory = Math.max(runtimeMaxMemory, maxMemory);
                maxUsedMemory = Math.max(maxUsedMemory, runtimeUsedMemory);

                logger.info("______________________________________");
                logger.info("Instance count: " + String.format("%,d", maxSize));
                logger.info("Instance per second: " + String.format("%,d", (int) instancesPerSecond));
                logger.info("Available Memory: " + String.format("%,d", runtimeMaxMemory / 1024 / 1024) + " MB");
                logger.info("Used Memory: " + String.format("%,d", runtimeUsedMemory / 1024 / 1024) + " MB");
                memoryTracker.getOut().println(String.format("%,d", (int) (System.currentTimeMillis() - start) / 1000) + "s instance count " + String.format("%,d", maxSize));
            }
        }
        //add element to memory, dump something to disk if necessary
        put(i);
    }


    @Override
    public void flush() {
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
        logger.info("Closing Instance Cache, flushing " + String.format("%,d", size()) + " instances to listeners.");
        for (IElementCacheListener l : listeners)
            l.startWorking(fifoQueue.clone());

        for (IElementCacheListener l : listeners) {
            try {
                l.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        memoryTracker.getOut().println("--------Instances---------");
        memoryTracker.getOut().println("Num: " + String.format("%,d", maxSize));
        memoryTracker.getOut().println("Time: " + String.format("%,d", (int) (System.currentTimeMillis() - start) / 1000) + "s");
        memoryTracker.getOut().println("Maximum instances per second: " + String.format("%,d", (int) maxTime));
        memoryTracker.getOut().println("Minimum instances per second: " + String.format("%,d", (int) minTime));
        memoryTracker.getOut().println("Maximum memory allocated: " + String.format("%,d", maxMemory / 1024 / 1024) + " MB");
        memoryTracker.getOut().println("Maximum memory used: " + String.format("%,d", maxUsedMemory / 1024 / 1024) + " MB");
        memoryTracker.getOut().println("--------------------------");
        logger.info("--------Instances---------");
        logger.info("Num: " + String.format("%,d", maxSize));
        logger.info("Time: " + String.format("%,d", (int) (System.currentTimeMillis() - start) / 1000) + "s");
        logger.info("Maximum instances per second: " + String.format("%,d", (int) maxTime));
        logger.info("Minimum instances per second: " + String.format("%,d", (int) minTime));
        logger.info("Maximum memory allocated: " + String.format("%,d", maxMemory / 1024 / 1024) + " MB");
        logger.info("Maximum memory used: " + String.format("%,d", maxUsedMemory / 1024 / 1024) + " MB");
        logger.info("--------------------------");
    }


    private boolean diskContains(IResource resource) {
        return new File(hashToFilename(resource.hashCode())).exists();
    }

    private T getFromDisk(IResource resource) {
        T res = null;
        File file = new File(hashToFilename(resource.hashCode()));
        if (file.exists()) {
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) {
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
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file))){
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
