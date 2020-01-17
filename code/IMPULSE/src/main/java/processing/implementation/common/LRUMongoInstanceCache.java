package main.java.processing.implementation.common;


import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import main.java.common.implementation.RDFInstance;
import main.java.processing.interfaces.IElementCache;
import main.java.processing.interfaces.IElementCacheListener;
import main.java.utils.LRUCache;
import main.java.utils.LongQueue;
import main.java.utils.MemoryTracker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import static main.java.utils.MainUtils.createFile;


/**
 *
 */
public class LRUMongoInstanceCache implements IElementCache<RDFInstance> {
    private static final Logger logger = LogManager.getLogger(LRUMongoInstanceCache.class.getSimpleName());
    private static final int loggingInterval = 1000000;
    private static final int memoryLoggingInterval = 10000000;

    private MongoClient mongoClient = new MongoClient();
    private MongoDatabase database = mongoClient.getDatabase("impulse-cache");
    private MongoCollection<Document> collection;
    //Least-Recently Used (LRU) elements stored in memory
    private LRUCache<Integer, RDFInstance> memoryCachedElements;
    //keep track of all elements to evict them later: First-in-First-out (FiFo).
    //allow more than Integer.MAX_VALUE elements, i.e., Integer.MAX_VALUE * Integer.MAX_VALUE values
    private LongQueue<Integer> fifoQueue;

    //maximum number of elements store in memory
    private int capacity;

    //maximum number of elements actually stored in memory/disk
    private long maxSize = 0;
    private MemoryTracker memoryTracker = new MemoryTracker("stats2", memoryLoggingInterval);

    //flush elements to each listener afterwards
    private List<IElementCacheListener> listeners;

    private Object sync = new Object();

    private long lastTime = System.currentTimeMillis();
    private long start = System.currentTimeMillis();

    private double maxTime = Double.MIN_VALUE;
    private double minTime = Double.MAX_VALUE;

    private long maxMemory = 0;
    private long maxUsedMemory = 0;

    private long cacheMisses = 0;


    /**
     * Constructor. Creates a cache with the given capacity
     *
     * @param capacity The capacity
     */
    public LRUMongoInstanceCache(int capacity, String mongoCollection) {
        //TODO check for existence
//        database.createCollection(mongoCollection);
        collection = database.getCollection(mongoCollection);
        memoryCachedElements = new LRUCache<>(capacity, 0.75f);
        listeners = new ArrayList<>();
        fifoQueue = new LongQueue<>();
        this.capacity = capacity;
    }

    public void setFifoQueue(LongQueue<Integer> fifoQueue) {
        this.fifoQueue = fifoQueue;
    }

    @Override
    public boolean contains(RDFInstance element) {
        return contains(element.getLocator());
    }

    @Override
    public boolean contains(Integer locator) {
        boolean contains = memoryCachedElements.containsKey(locator);
        if (!contains) {
            RDFInstance element = getFromDB(locator);
            if (element != null) {
                put(element);
                return true;
            } else
                return false;
        } else
            return true;
    }

    @Override
    public RDFInstance get(Integer locator) {
        //element not in memory
        if (!memoryCachedElements.containsKey(locator)) {
            //element not on disk
            synchronized (sync) {
                RDFInstance element = getFromDB(locator);
                if (element != null)
                    put(element);
                return element;
            }

        } else
            return memoryCachedElements.get(locator);

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
        RDFInstance eldestElement = memoryCachedElements.getEldestEntry().getValue();
        //do not delete old element but store on disk
        saveToMongo(eldestElement);
        memoryCachedElements.remove(eldestElement.getLocator());
    }

    private void put(RDFInstance element) {
        // if memory is full -> move eldest entry to disk
        synchronized (sync) {
            if (memoryCachedElements.size() >= capacity)
                removeEldest();

            //finally, add new element to cache
            memoryCachedElements.put(element.getLocator(), element);
        }
    }


    /**
     * ArrayDeque.MAX_ARRAY_SIZE = 2147483639
     *
     * @param resource
     */
    private void addToQueue(Integer resource) {
        synchronized (sync) {
            fifoQueue.add(resource);
        }
    }


    @Override
    public void add(RDFInstance element) {
        if (!contains(element)) {
            //new resource, add to queue
            addToQueue(element.getLocator());
            //increase total counter
            maxSize++;
            if (size() % loggingInterval == 0) {
                logger.debug("--------------------------------------");
                logger.debug("Instances parsed: " + String.format("%,d", size()));
                logger.debug("Memory cached files: " + String.format("%,d", memoryCachedElements.size()));
                logger.debug("Queue Size: " + String.format("%,d", fifoQueue.size()));
                //System.out.format("Instances parsed: %08d    \r", size());
            }

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
        put(element);
    }


    @Override
    public void flush() {
        memoryCachedElements = new LRUCache<>(capacity, 0.75f);
        fifoQueue = new LongQueue<>();
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
        logger.info("Cache misses: " + cacheMisses);
    }

    @Override
    public void flushAll(String outfile) {
        logger.info("Flushing cache to mongoDB");
        long i = 0L;
        for (RDFInstance element : memoryCachedElements.values()) {
            saveToMongo(element);
            i++;
            if (i % 1000 == 0)
                logger.debug("flushed: " + i);
        }
        logger.info("Exporting " + fifoQueue.size() + " subject URIs");
        PrintStream out = null;
        try {
            out = new PrintStream(new FileOutputStream(createFile(outfile)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }


        while (fifoQueue.size() > 0)
            out.println(fifoQueue.poll());
        out.close();
    }


//    private boolean mongoContains(Integer locator) {
//        Document document = collection.find(new Document("locator", locator)).first();
//        return document != null;
//    }

    private RDFInstance getFromDB(Integer locator) {
        Document document = collection.find(new Document(RDFInstance.LOCATOR_KEY, locator)).first();
        if (document != null)
            return new RDFInstance(document);
        else
            return null;
    }

    private void saveToMongo(RDFInstance element) {
        Document document = element.toDocument();
        collection.insertOne(document);
    }

}
