package main.java.processing.implementation;


import main.java.common.implementation.RDFInstance;
import main.java.common.interfaces.IInstanceElement;
import main.java.common.interfaces.IQuint;
import main.java.processing.implementation.common.DataItemBuffer;
import main.java.processing.implementation.parsing.MOVINGParser;
import main.java.processing.interfaces.IElementCache;
import main.java.processing.interfaces.IElementCacheListener;
import main.java.utils.MainUtils;

import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Blume Till on 07.11.2019.
 */
public class ContextHarvester extends Harvester implements IElementCacheListener {

    Set<String> contexts;
    long filteredInstances = 0L;
    long filteredQuads = 0L;

    public ContextHarvester(String name, MOVINGParser parser, IElementCache<IInstanceElement> cache, DataItemBuffer dataItemBuffer, Set<String> contexts) {
        super(name, parser, cache, dataItemBuffer);
        this.contexts = new HashSet<>();
        contexts.forEach(C -> {
            try {
                this.contexts.add(MainUtils.normalizeURL(C));
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        });
        logger.debug("loaded " +  String.format("%,d", contexts.size()) + " sources.");
    }

    public ContextHarvester(String name, MOVINGParser parser, IElementCache<IInstanceElement> cache, DataItemBuffer dataItemBuffer) {
        super(name, parser, cache, dataItemBuffer);
    }


    @Override
    public IInstanceElement getInstance(Integer locator) {
        return filterInstance(super.getInstance(locator));
    }

    IInstanceElement filterInstance(IInstanceElement iInstanceElement) {
        if (iInstanceElement == null)
            return null;
        IInstanceElement filteredInstance = new RDFInstance(iInstanceElement.getLocator());
        for (IQuint quint : iInstanceElement.getOutgoingQuints()) {
            try {
                if (contexts.contains(MainUtils.normalizeURL(quint.getContext().toString())))
                    filteredInstance.addOutgoingQuint(quint);
                else
                    filteredQuads++;
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        }
        if (filteredInstance.getOutgoingQuints().size() > 0)
            return filteredInstance;

        filteredInstances++;
        return null;
    }

    @Override
    public void finished() {
        logger.info("Harvesting of " + successful + "/" + (successful + erroneous) + " instances finished successfully, " + filteredInstances + " instances filtered!" +
                " Flushing them to sinks...");
        logger.debug("Filtered Quads: " + filteredQuads);
        jsonCache.flush();
        logger.info("Closing sinks...");
        jsonCache.close();
    }
}
