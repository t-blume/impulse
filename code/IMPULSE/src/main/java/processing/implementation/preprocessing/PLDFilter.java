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

public class PLDFilter implements IQuintProcessor {
    private static final Logger logger = LogManager.getLogger(PLDFilter.class.getSimpleName());

    private long counter = 0;

    private Set<String> payLevelDomains;


    public PLDFilter(Set<String> contexts) {
        payLevelDomains = new HashSet<>();
        contexts.forEach(C -> {
            try {
                payLevelDomains.add(MainUtils.extractPLD(MainUtils.normalizeURL(C)));
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public List<IQuint> processQuint(IQuint q) throws URISyntaxException {
        List<IQuint> quints = new LinkedList<>();
        if (payLevelDomains.contains(MainUtils.extractPLD(MainUtils.normalizeURL(q.getContext().toString()))))
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
        return "PLDFilter{" +
                "payLevelDomains: " + payLevelDomains.size() +
                '}';
    }
}
