package main.java.processing.implementation;


import main.java.common.data.model.DataItem;
import main.java.common.interfaces.IInstanceElement;
import main.java.processing.implementation.common.DataItemCache;
import main.java.processing.implementation.parsing.MOVINGParser;
import main.java.processing.interfaces.IElementCacheListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by Blume Till on 07.11.2016.
 */
public class Harvester implements IElementCacheListener<IInstanceElement> {
    private static final Logger logger = LogManager.getLogger(Harvester.class.getSimpleName());

    //Parser that used the imported mapping file to convert RDF instances to JSON instances
    private MOVINGParser parser;

    //temporary in-memory storage for parsed JSON instances
    private DataItemCache jsonCache;

    private int limit;

    int successfull = 0;
    int errornous = 0;

    public Harvester(MOVINGParser parser, DataItemCache jsonCache, int limit) {
        this.parser = parser;
        this.jsonCache = jsonCache;
        this.limit = limit;
    }


    @Override
    public void elementFlushed(IInstanceElement instance) {
//        System.out.println(instance.getLocator());
//        System.out.println(instance.getOutgoingQuints());
        DataItem dataItem = parser.convertInstance2JSON(instance);

        if (dataItem != null && dataItem.getMetadataPersons() != null) {
            jsonCache.add(dataItem);
            if(jsonCache.size() > limit) {
                logger.info("Flushing intermediate chunk of " + jsonCache.size() + " items to sinks!");
                jsonCache.flush();
            }
            successfull++;
        } else
            errornous++;

    }

    @Override
    public void finished() {
        logger.info("Harvesting of " + successfull + "/" + (successfull + errornous) + " finished successfully, flushing to sinks...");
        jsonCache.flush();
    }
}
