package main.java.processing.implementation.preprocessing;

import main.java.common.interfaces.IQuint;
import main.java.input.interfaces.IQuintListener;
import main.java.input.interfaces.IQuintSourceListener;
import main.java.processing.interfaces.IQuintPipeline;
import main.java.processing.interfaces.IQuintProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URISyntaxException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * Basic implementation of a {@link IQuintPipeline} which may get its
 * {@link IQuint}s from sources at which it is registered. Quints will be
 * processed by the internal processors in the order they were added. For all
 * resulting Quints, listeners will be notified
 *
 * @author Bastian
 * @author Blume Till
 */
public class BasicQuintPipeline implements IQuintPipeline, IQuintSourceListener {
    private static final Logger logger = LogManager.getLogger(BasicQuintPipeline.class.getSimpleName());

    private List<IQuintListener> listeners;
    private List<IQuintProcessor> processors;

    /**
     * Constructor
     */
    public BasicQuintPipeline() {
        listeners = new ArrayList<>();
        processors = new ArrayList<>();
    }

    @Override
    public void process(IQuint i) throws URISyntaxException {
        Queue<IQuint> temp = new ArrayDeque<>();
        Queue<IQuint> current = new ArrayDeque<>();

        //System.out.println("Quint: " + i);
        temp.add(i);
        for (IQuintProcessor p : processors) {
            while (!temp.isEmpty()) {
                IQuint q = temp.poll();
                List<IQuint> list = p.processQuint(q);
                //System.out.println(list);
                //current.add(q);
                current.addAll(list);
            }
            Queue<IQuint> swap = temp;
            temp = current;
            current = swap;
        }


        for (IQuint q : temp)
            notifyListeners(q);

    }

    private void notifyListeners(IQuint q) {
        for (IQuintListener l : listeners)
            l.finishedQuint(q);

    }

    @Override
    public void addProcessor(IQuintProcessor p) {
        processors.add(p);
    }

    @Override
    public void removeProcessor(IQuintProcessor p) {
        processors.remove(p);
    }

    @Override
    public List<IQuintProcessor> getPipeline() {
        return processors;
    }

    @Override
    public void registerQuintListener(IQuintListener l) {
        listeners.add(l);
    }

    @Override
    public void pushedQuint(IQuint quint) {
        try {
            process(quint);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void microBatch() {
      //  logger.debug("Micro batch initiated");
        for (IQuintListener l : listeners)
            l.microBatch();
    }

    @Override
    public void sourceClosed() {
        logger.debug("Pre-processing is finished!");
        for (IQuintListener l : listeners)
            l.finished();

        for (IQuintProcessor p : processors)
            p.finished();

    }

    @Override
    public void sourceStarted() {
        logger.info("Pre-processing pipeline configuration:" + toString());
    }

    @Override
    public String toString() {
        String pipeline = "\n";
        for (int i = 0; i < processors.size(); i++)
            pipeline += "\t\t" + (i + 1) + ": " + processors.get(i).toString() + "\n";

        return pipeline.substring(0, pipeline.length() - 1);
    }
}
