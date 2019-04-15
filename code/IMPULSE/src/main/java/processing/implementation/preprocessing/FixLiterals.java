package main.java.processing.implementation.preprocessing;

import main.java.common.interfaces.IQuint;
import main.java.processing.interfaces.IQuintProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.List;

public class FixLiterals implements IQuintProcessor {
    private static final Logger logger = LogManager.getLogger(FixLiterals.class.getSimpleName());

    boolean tryFix = false;

    public FixLiterals(boolean tryFix) {
        this.tryFix = tryFix;
    }

    long filteredLiterals = 0;
    long fixedLiterals = 0;

    @Override
    public List<IQuint> processQuint(IQuint q) {
        List<IQuint> quints = new LinkedList<>();
                /*
        TODO: check if object matches the mentioned regex pattern and then try to fix it
         */
        String test = q.getObject().toN3();
        logger.debug(test);
        if (test.matches("<\".*\"(@[a-z]{2})?>")) {
            logger.debug("Invalid literal");
            if (tryFix) {
                //try to fix literal
            }

        } else
            quints.add(q);

        return quints;
    }

    @Override
    public void finished() {
        logger.info("finished(): Fixed " + fixedLiterals + " literals, filtered " + filteredLiterals + " literals.");
    }

    @Override
    public String toString() {
        return "FixLiterals{" +
                "tryFix=" + tryFix +
                '}';
    }
}
