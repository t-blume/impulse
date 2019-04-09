package main.java.input.implementation;

import main.java.common.implementation.Quad;
import main.java.input.interfaces.IQuintSource;
import main.java.input.interfaces.IQuintSourceListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * TODO: @Sven: Implement
 * http://docs.rdf4j.org/programming/#_using_a_repository_repositoryconnections
 */
public class RDF4JQuadSource implements IQuintSource {

    private static final Logger logger = LogManager.getLogger(RDF4JQuadSource.class.getSimpleName());

    private List<IQuintSourceListener> listeners;
    private int counter = 0;
    private String repositoryURL;

    public RDF4JQuadSource(String repositoryURL) {
        this.repositoryURL = repositoryURL;
    }


    /**
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        logger.info("Source closed, processed " + counter + " quints");
        //TODO close all open connections
    }

    @Override
    public void start() {
        logger.info("Source started (" + repositoryURL+ ")");

        //TODO: Opens a connections to the repository

        //get all subject URIs
        List<String> subjectURIs = getAllSubjectURIs();

        if (subjectURIs == null || subjectURIs.isEmpty()) {
            logger.info("Empty Repository provided!");
            try {
                close();
            } catch (IOException e) {
                logger.error(e.getLocalizedMessage());
            }
        }
        //start iterating through the whole repository
        for (String subjectURI : subjectURIs) {
            //retrieves all quads (recursively) related to the subject URI and sends
            //them to teh listener
            getAllQuadsForSubjectRecursive(getAllQuadsForSubject(subjectURI));
            //signal that now all relevant information is in the instance cache
            for (IQuintSourceListener l : listeners)
                l.microBatch();
        }
    }

    @Override
    public void registerQuintListener(IQuintSourceListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeQuintListener(IQuintSourceListener listener) {
        listeners.remove(listener);
    }


    /**
     * Uses the open connection to the repository and returns a distinct list of URIs
     * that appear as subject in the repository
     * <p>
     * Remarks:
     * String return type may need change,
     * this query can take a while
     *
     * @return
     */
    private List<String> getAllSubjectURIs() {
        return null;
    }


    /**
     * Uses the open connection to the repository and returns a distinct list of Quads
     * that have the provided subjectURI as subject.
     *
     * @param subjectURI
     * @return
     */
    private Set<Quad> getAllQuadsForSubject(String subjectURI) {

        //TODO retrieve all quads
        Set<Quad> retrievedQuads = new HashSet<>();
        //notify listeners of new quad
        retrievedQuads.forEach(Q -> {
            for (IQuintSourceListener l : listeners)
                l.pushedQuint(Q);
        });

        return null;
    }


    /**
     * Recursivly retrieve all instance information starting from the subject URI
     *
     * @param inputQuads
     * @return
     */
    private void getAllQuadsForSubjectRecursive(Set<Quad> inputQuads) {
        if (inputQuads == null)
            return;

        for (Quad inputQuad : inputQuads) {
            //TODO: could be literal, only handle valid URIs
            //TODO: watch out for the < >
            Set<Quad> tmpQuads = getAllQuadsForSubject(inputQuad.getObject().toString());
            getAllQuadsForSubjectRecursive(tmpQuads);
        }
    }
}
