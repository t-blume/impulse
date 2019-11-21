package main.java.processing.implementation;


import main.java.common.data.model.DataItem;
import main.java.common.interfaces.IInstanceElement;
import main.java.common.interfaces.IResource;
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
    Logger logger = LogManager.getLogger(Harvester.class.getSimpleName());

    void setLogger(Logger logger){
        this.logger = logger;
    }

    //Parser that used the imported mapping file to convert RDF instances to JSON instances
    private MOVINGParser parser;

    //temporary in-memory storage for parsed JSON instances before flushing
    DataItemBuffer jsonCache;

    IElementCache<IInstanceElement> cache;

    LongQueue<IResource> fifoQueue;

    int successful = 0;
    int erroneous = 0;

    public Harvester(MOVINGParser parser, IElementCache<IInstanceElement> cache, DataItemBuffer dataItemBuffer) {
        this.parser = parser;
        this.cache = cache;
        this.jsonCache = dataItemBuffer;
    }

    public MOVINGParser getParser() {
        return parser;
    }

    @Override
    public void startWorking(LongQueue<IResource> fifoQueue) {
        this.fifoQueue = fifoQueue;
        logger.debug("Starting to iterate over " + fifoQueue.size() + " instances.");
        this.start();
    }

    public void run() {
        logger.debug("Running like hell...");
        IResource nextResource;
        while ((nextResource = fifoQueue.poll()) != null) {
            handleInstance(getInstance(nextResource));
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

    public IInstanceElement getInstance(IResource resource){
        return cache.get(resource);
    }

    public void finished() {
        logger.info("Harvesting of " + successful + "/" + (successful + erroneous) + " finished successfully, flushing to sinks...");
        jsonCache.flush();
    }
}
