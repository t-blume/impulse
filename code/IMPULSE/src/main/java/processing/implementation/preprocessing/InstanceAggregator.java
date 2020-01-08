package main.java.processing.implementation.preprocessing;


import main.java.common.implementation.RDFInstance;
import main.java.common.interfaces.IInstanceElement;
import main.java.common.interfaces.IQuint;
import main.java.common.interfaces.IResource;
import main.java.input.interfaces.IQuintListener;
import main.java.processing.interfaces.IElementCache;
import main.java.utils.MemoryTracker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;


/**
 * Aggregates information of instances
 *
 * @author Bastian
 * @editor Till
 */
public class InstanceAggregator implements IQuintListener {
    private static final Logger logger = LogManager.getLogger(InstanceAggregator.class.getSimpleName());
    private MemoryTracker memoryTracker = new MemoryTracker();

    private Map<IResource, Integer> largeInstances = new HashMap<>();
    private IElementCache<IInstanceElement> window;

    /**
     * @param window
     */
    public InstanceAggregator(IElementCache<IInstanceElement> window) {
        this.window = window;
    }

    @Override
    public void finishedQuint(IQuint i) {
        addQuint2Cache(i, true);
    }

    @Override
    public void microBatch() {
        //flushes all instances out of the window, clears window afterwards to safe space
        window.flush();
    }

    protected IInstanceElement createInstance(IQuint quint) {
        return new RDFInstance(quint.getSubject());
    }

    protected void addQuint2Cache(IQuint quint, boolean asOutgoing) {
        IInstanceElement element = createInstance(quint);
        if (window.contains(element.getLocator()))
            element = window.get(element.getLocator());

        if (asOutgoing)
            element.addOutgoingQuint(quint);
        else
            element.addIncomingQuint(quint);

        window.add(element);
        if (element.getOutgoingQuints().size() % 100000 == 0) {
            if (!largeInstances.containsKey(element.getLocator())) {
                logger.debug("------------------------");
                logger.debug("Large Instance: " + element.getLocator() + " has size: " + element.getOutgoingQuints().size());
                largeInstances.put(element.getLocator(), element.getOutgoingQuints().size());
                logger.info("Used Memory: " + String.format("%,d", memoryTracker.getReallyUsedMemory() / 1024 / 1024) + " MB");
            } else
                largeInstances.put(element.getLocator(), largeInstances.get(element.getLocator()) + 1);

        }
    }

    @Override
    public void finished() {
        logger.debug("Finished aggregating");
        logger.debug("Number of large instances: " + largeInstances.size());
        int max = 0;
        int sum = 0;
        for (Map.Entry<IResource, Integer> instance : largeInstances.entrySet()) {
            if (instance.getValue() > max)
                max = instance.getValue();
            sum += instance.getValue();
        }
        logger.debug("Largest instance size: " + max);
        logger.debug("Avg. large instance site: " + sum / largeInstances.size());

        window.close();
    }
}
