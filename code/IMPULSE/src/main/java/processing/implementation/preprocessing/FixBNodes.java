package main.java.processing.implementation.preprocessing;


import main.java.common.implementation.Quad;
import main.java.common.interfaces.IQuint;
import main.java.processing.interfaces.IQuintProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.List;


public class FixBNodes implements IQuintProcessor {
    private static final Logger logger = LogManager.getLogger(FixBNodes.class.getSimpleName());

    private boolean tryFix;
    private String prefix;

    public FixBNodes(boolean tryFix, String prefix) {
        this.tryFix = tryFix;
        this.prefix = prefix;
    }

    static long filteredLiterals = 0;
    static long fixedBlankNodes = 0;
    static long fixedLiterals = 0;
    static long removedQuints = 0;

    @Override
    public List<IQuint> processQuint(IQuint q) {
        List<IQuint> quints = new LinkedList<>();

        //It is an older code, Sir, but it checks out
        if (tryFix) {
            String c = q.getContext();
            String s = q.getSubject();
            String p = q.getPredicate();
            String o = q.getObject();

            String context = deanon(c, prefix);
            String subject = deanon(s, c);
            String predicate = deanon(p, c);
            String object = deanon(o, c);

            IQuint fixedQuint = new Quad(subject, predicate, object, context);
            quints.add(fixedQuint);
        } else
            quints.add(q);

        return quints;
    }

    private String deanon(String resource, String context) {
        String resourceText = resource.toString();

        if (resource.startsWith("<_:")) {
            String newPrefix;
            if (context != null) {
                newPrefix = context.toString();
                // In case the context itself was a blank node -> use prefix
                if (newPrefix.startsWith("_:"))
                    newPrefix = prefix;

            } else
                newPrefix = prefix;

            if (!(newPrefix.endsWith("/") || newPrefix.endsWith("#")))
                newPrefix = newPrefix + "/";

            //remove possible blank node prefix
            resourceText = resourceText.replaceAll("_:", "");
            fixedBlankNodes++;
            return newPrefix + resourceText;
        } else
            return resource;
    }


    public static long getFixedLiterals() {
        return fixedLiterals;
    }

    public static long getFixedBlankNodes() {
        return fixedBlankNodes;
    }

    public static long getRemovedQuints() {
        return removedQuints;
    }


    private String fixLiteral(String l) {
        if (!l.matches("\".*\"(@[a-z]+)?")) {
            String test = l;
            if (test.contains("@")) {
                int index = test.indexOf("@");
                test = test.substring(0, index);
            }
            return "<" + l.toString() + ">";
        }
        return null;
    }


    @Override
    public void finished() {
        logger.info("finished(): Fixed " + fixedLiterals + " literals, filtered " + filteredLiterals + " literals.");
    }

    @Override
    public String toString() {
        return "FixBNodes{" +
                "tryFix=" + tryFix +
                '}';
    }
}
