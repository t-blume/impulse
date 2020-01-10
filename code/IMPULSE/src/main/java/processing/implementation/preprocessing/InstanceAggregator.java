package main.java.processing.implementation.preprocessing;


import main.java.common.implementation.RDFInstance;
import main.java.common.interfaces.IInstanceElement;
import main.java.common.interfaces.IQuint;
import main.java.input.interfaces.IQuintListener;
import main.java.processing.interfaces.IElementCache;
import main.java.utils.MemoryTracker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    private IElementCache<IInstanceElement> window;

    /**
     * @param window
     */
    public InstanceAggregator(IElementCache<IInstanceElement> window) {
        this.window = window;
    }

    @Override
    public void finishedQuint(IQuint quint) {
        addQuint2Cache(quint, true);
    }

    @Override
    public void microBatch() {
        //flushes all instances out of the window, clears window afterwards to safe space
        window.flush();
    }

    protected IInstanceElement createInstance(IQuint quint) {
        return new RDFInstance(quint.getSubject().hashCode());
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
    }

    @Override
    public void finished() {
        logger.debug("Finished aggregating");
        window.close();
    }
}
