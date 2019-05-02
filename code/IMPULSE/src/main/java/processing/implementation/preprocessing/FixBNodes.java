package main.java.processing.implementation.preprocessing;

//import main.java.common.implementation.NodeResource;
//import main.java.common.implementation.Quad;
//import main.java.common.interfaces.IQuint;
//import main.java.common.interfaces.IResource;
//import main.java.processing.interfaces.IQuintProcessor;
//import main.java.utils.DataCleansing;
//import org.apache.jena.iri.IRI;
//import org.apache.jena.iri.IRIFactory;
//import org.apache.jena.iri.Violation;
//import org.apache.jena.rdf.model.*;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import org.semanticweb.yars.nx.Node;
//
//
//import java.util.Iterator;
//import java.util.LinkedList;
//import java.util.List;

import main.java.common.implementation.NodeResource;
import main.java.common.implementation.Quad;
import main.java.common.interfaces.IQuint;
import main.java.common.interfaces.IResource;
import main.java.processing.interfaces.IQuintProcessor;
import org.apache.jena.iri.IRI;
import org.apache.jena.iri.IRIFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Resource;

import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.List;

import static main.java.utils.MainUtils.editStrings;


public class FixBNodes implements IQuintProcessor {
    private static final Logger logger = LogManager.getLogger(FixBNodes.class.getSimpleName());

    private boolean tryFix = false;
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

            NodeResource c = (NodeResource) q.getContext();
            NodeResource s = (NodeResource) q.getSubject();
            NodeResource p = (NodeResource) q.getPredicate();
            NodeResource o = (NodeResource) q.getObject();


            IResource context = deanon(c, new NodeResource(new Resource(prefix)));
            IResource subject = deanon(s, c);
            IResource predicate = deanon(p, c);
            IResource object = deanon(o, c);


            IQuint fixedQuint = new Quad(subject, predicate, object, context);

            quints.add(fixedQuint);
        } else
            quints.add(q);

        return quints;
    }

    private IResource deanon(NodeResource resource, IResource context) {
        String resourceText = resource.toString();


      //  if (resource.getNode() instanceof BNode) {
        if (resource.getNode().toString().startsWith("<_:")){


//        if (!resourceText.matches("(http(s)?|(ftp)):\\/\\/.*")) {
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
            return new NodeResource(new Resource(newPrefix + resourceText));
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


    private IResource fixLiteral(IResource l) throws UnsupportedEncodingException {
//
        if (!l.toN3().matches("\".*\"(@[a-z]+)?")) {
            IRIFactory iriFactory = IRIFactory.iriImplementation();
            String test = l.toN3();
            if (test.contains("@")) {

                int index = test.indexOf("@");

                test = test.substring(0, index);


            }
//            test = test.replace("<\"", "\"");
//            test = test.replace("\">", "\"");
//

            test = test.trim();
//            System.out.println("test " + test);
//
            IRI iri = iriFactory.create(test);


            return new NodeResource(new Resource("<" + l.toString() + ">"));
//            return new NodeResource(new Resource("\"" + l.toString() + "\""));
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
