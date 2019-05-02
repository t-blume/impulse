package main.java.processing.implementation.preprocessing;

import main.java.common.interfaces.IQuint;
import main.java.processing.interfaces.IQuintProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class ContextFilter implements IQuintProcessor {
    private static final Logger logger = LogManager.getLogger(ContextFilter.class.getSimpleName());

    private int counter = 0;
    private Set<String> contexts;

    public ContextFilter(Set<String> contexts) {
        this.contexts = contexts;
    }

    @Override
    public List<IQuint> processQuint(IQuint q) {
        List<IQuint> quints = new LinkedList<>();
      // quints.add(q);
        if(contexts.contains(q.getContext().toString()))
            quints.add(q);
        else
            counter++;

        return quints;
    }

    @Override
    public void finished() {
        logger.info("Filtered " + counter + " quints");
    }

    @Override
    public String toString() {
        return "ContextFilter{" +
                "contexts: " + contexts.size() +
                '}';
    }
}
