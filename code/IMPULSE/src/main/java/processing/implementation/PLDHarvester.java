package main.java.processing.implementation;


import main.java.common.implementation.RDFInstance;
import main.java.common.interfaces.IInstanceElement;
import main.java.common.interfaces.IQuint;
import main.java.processing.implementation.common.DataItemBuffer;
import main.java.processing.implementation.parsing.MOVINGParser;
import main.java.processing.interfaces.IElementCache;
import main.java.processing.interfaces.IElementCacheListener;
import main.java.utils.MainUtils;
import org.apache.logging.log4j.LogManager;

import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Blume Till on 07.11.2019.
 */
public class PLDHarvester extends ContextHarvester implements IElementCacheListener {
    public PLDHarvester(MOVINGParser parser, IElementCache<IInstanceElement> cache, DataItemBuffer dataItemBuffer, Set<String> contexts) {
        super(parser, cache, dataItemBuffer);
        this.contexts = new HashSet<>();
        contexts.forEach(C -> {
            try {
                this.contexts.add(MainUtils.extractPLD(MainUtils.normalizeURL(C)));
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        });
        setLogger(LogManager.getLogger(PLDHarvester.class.getSimpleName()));
    }


    @Override
    IInstanceElement filterInstance(IInstanceElement iInstanceElement) {
        if (iInstanceElement == null)
            return null;
        IInstanceElement filteredInstance = new RDFInstance(iInstanceElement.getLocator());
        for (IQuint quint : iInstanceElement.getOutgoingQuints()) {
            try {
                if (contexts.contains(MainUtils.extractPLD(MainUtils.normalizeURL(quint.getContext().toString()))))
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
}
