package main.java.processing.implementation;


import main.java.common.data.model.DataItem;
import main.java.common.interfaces.IInstanceElement;
import main.java.processing.implementation.common.DataItemBuffer;
import main.java.processing.implementation.parsing.MOVINGParser;
import main.java.processing.interfaces.IElementCache;
import main.java.processing.interfaces.IElementCacheListener;
import main.java.utils.LongQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by Blume Till on 07.11.2016.
 */
public class Harvester extends Thread implements IElementCacheListener {

    Logger logger;

    //Parser that used the imported mapping file to convert RDF instances to JSON instances
    private MOVINGParser parser;

    //temporary in-memory storage for parsed JSON instances before flushing
    DataItemBuffer jsonCache;

    IElementCache<IInstanceElement> cache;

    LongQueue<Integer> fifoQueue;

    int successful = 0;
    int erroneous = 0;

    public Harvester(String name, MOVINGParser parser, IElementCache<IInstanceElement> cache, DataItemBuffer dataItemBuffer) {
        this.parser = parser;
        this.cache = cache;
        this.jsonCache = dataItemBuffer;
        this.cache.registerCacheListener(this);
        this.logger = LogManager.getLogger(name);

    }

    public MOVINGParser getParser() {
        return parser;
    }

    @Override
    public void startWorking(LongQueue<Integer> fifoQueue) {
        this.fifoQueue = fifoQueue;
        logger.debug("Starting to iterate over " + String.format("%,d", fifoQueue.size()) + " instances.");
        this.start();
    }

    public void run() {
        logger.debug("Running like hell...");
        Integer nextLocator;
        while ((nextLocator = fifoQueue.poll()) != null) {
            handleInstance(getInstance(nextLocator));
        }
        finished();
    }

    void handleInstance(IInstanceElement instance) {
        if(instance != null){
            DataItem dataItem = parser.convertInstance2JSON(instance);
            if (dataItem != null && dataItem.getMetadataPersons() != null) {
                jsonCache.add(dataItem);
                successful++;
            } else
                erroneous++;
        }else
            erroneous++;
    }

    public IInstanceElement getInstance(Integer locator){
        return cache.get(locator);
    }

    public void finished() {
        logger.info("Harvesting of " + String.format("%,d", successful) + "/" + String.format("%,d", successful + erroneous) +
                " finished successfully, flushing to sinks...");
        jsonCache.flush();
        logger.info("Closing sinks...");
        jsonCache.close();
    }
}
