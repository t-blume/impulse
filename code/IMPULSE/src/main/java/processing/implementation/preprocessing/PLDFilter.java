package main.java.processing.implementation.preprocessing;

import main.java.common.interfaces.IQuint;
import main.java.processing.interfaces.IQuintProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class PLDFilter implements IQuintProcessor {
    private static final Logger logger = LogManager.getLogger(PLDFilter.class.getSimpleName());

    private int counter = 0;

    private Set<String> paylevelDomains;


    public PLDFilter(Set<String> contexts) {
        paylevelDomains = new HashSet<>();
        contexts.forEach(C -> {
            try {
                paylevelDomains.add(new URI(C).getHost());
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public List<IQuint> processQuint(IQuint q) {
        List<IQuint> quints = new LinkedList<>();
        try {
            if (paylevelDomains.contains(new URI(q.getContext().toString()).getHost()))
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
        return "PLDFilter{" +
                "paylevelDomains: " + paylevelDomains.size() +
                '}';
    }
}
