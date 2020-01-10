package main.java.processing.implementation.preprocessing;

import main.java.common.interfaces.IQuint;
import main.java.processing.interfaces.IQuintProcessor;
import main.java.utils.MainUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class ContextFilter implements IQuintProcessor {
    private static final Logger logger = LogManager.getLogger(ContextFilter.class.getSimpleName());

    private long counter = 0;
    private Set<String> contexts;

    public ContextFilter(Set<String> contexts) {

        this.contexts = new HashSet<>();
        contexts.forEach(C -> {
            try {
                this.contexts.add(MainUtils.normalizeURL(C));
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public List<IQuint> processQuint(IQuint q) {
        List<IQuint> quints = new LinkedList<>();
        try {
            if(contexts.contains(MainUtils.normalizeURL(q.getContext())))
                quints.add(q);
            else
                counter++;
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

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
