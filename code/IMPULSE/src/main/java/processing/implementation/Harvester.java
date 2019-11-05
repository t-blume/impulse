package main.java.processing.implementation;


import main.java.common.data.model.DataItem;
import main.java.common.interfaces.IInstanceElement;
import main.java.processing.implementation.common.DataItemBuffer;
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

    //temporary in-memory storage for parsed JSON instances before flushing
    private DataItemBuffer jsonCache;


    private int successful = 0;
    private int erroneous = 0;

    public Harvester(MOVINGParser parser, DataItemBuffer dataItemBuffer) {
        this.parser = parser;
        this.jsonCache = dataItemBuffer;
    }


    @Override
    public void elementFlushed(IInstanceElement instance) {
        DataItem dataItem = parser.convertInstance2JSON(instance);
        if (dataItem != null && dataItem.getMetadataPersons() != null) {
            jsonCache.add(dataItem);
            successful++;
        } else
            erroneous++;

    }

    @Override
    public void finished() {
        logger.info("Harvesting of " + successful + "/" + (successful + erroneous) + " finished successfully, flushing to sinks...");
        jsonCache.flush();
    }
}
