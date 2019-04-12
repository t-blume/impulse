package main.java.input.implementation;

import main.java.MyTripleStore;
import main.java.common.implementation.Quad;
import main.java.input.interfaces.IQuintSource;
import main.java.input.interfaces.IQuintSourceListener;
import org.apache.commons.math3.geometry.spherical.twod.Edge;
import org.apache.commons.math3.stat.descriptive.moment.SemiVariance;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.ExceptionConvertingIteration;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.eclipse.rdf4j.sail.SailException;
import sun.security.provider.certpath.Vertex;

import java.io.IOException;
import java.util.*;

/**
 * TODO: @Sven: Implement
 * http://docs.rdf4j.org/programming/#_using_a_repository_repositoryconnections
 */
public class RDF4JQuadSource implements IQuintSource {

    private static final Logger logger = LogManager.getLogger(RDF4JQuadSource.class.getSimpleName());

    private List<IQuintSourceListener> listeners;
    private int counter = 0;
    private String repositoryURL;
    private String repositoryID;
    Repository repo;

    public RDF4JQuadSource(String repositoryURL, String repositoryID) {
        this.repositoryURL = repositoryURL;
        this.repositoryID = repositoryID;
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
        logger.info("Source started (" + repositoryURL + ")");

        //TODO: Opens a connections to the repository

        this.repo = new HTTPRepository(repositoryURL, repositoryID);
//        repo.initialize();
        repo.init();
        RepositoryConnection con = repo.getConnection();


        //get all subject URIs
        //TODO check if Hashset or List
        HashSet<String> subjectURIs =  getAllSubjectURIs(con);


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
            getAllQuadsForSubjectRecursive(getAllQuadsForSubject(subjectURI, con));
            //signal that now all relevant information is in the instance cache
            for (IQuintSourceListener l : listeners)
                l.microBatch();
        }
        try {
            close();
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage());
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
     * @return List<String>
     */
    private HashSet<String> getAllSubjectURIs(RepositoryConnection conn) {
        HashSet<String> allSubjects = new HashSet<>();
        // We do a SPARQL SELECT-query that retrieves all subjects from the repository
        String queryString = "SELECT ?subject \n";
        queryString += "WHERE {?subject ?predicate ?object} \n";

        TupleQuery query = conn.prepareTupleQuery(queryString);

        // A QueryResult is also an AutoCloseable resource, so make sure it gets closed when done.
        try (TupleQueryResult result = query.evaluate()) {
            // we just iterate over all solutions in the result...
            while (result.hasNext()) {
                BindingSet solution = result.next();
                // ... and print out the value of the variable binding for ?s and ?n
                System.out.println("Subject = " + solution.getValue("subject"));
                allSubjects.add(solution.getValue("subject").stringValue());
            }
        }
        return allSubjects;
    }


    /**
     * Uses the open connection to the repository and returns a distinct list of Quads
     * that have the provided subjectURI as subject.
     *
     * @param subjectURI
     * @return
     */
    private Set<Quad> getAllQuadsForSubject(String subjectURI, RepositoryConnection conn) {
        //TODO retrieve all quads


        String queryString = "SELECT ?subject ?predicate ?object ?context \n";
        queryString += "WHERE {?subject:" + subjectURI + "} \n";

        TupleQuery query = conn.prepareTupleQuery(queryString);

        // A QueryResult is also an AutoCloseable resource, so make sure it gets closed when done.
        try (TupleQueryResult result = query.evaluate()) {
            // we just iterate over all solutions in the result...
            while (result.hasNext()) {
                BindingSet solution = result.next();
                // ... and print out the value of the variable binding for ?s and ?n
                System.out.println("Subject = " + solution.getValue("subject") + " Predicate = "+ solution.getValue("predicate")+ " Object = "+ solution.getValue("Object")+ " Context = "+ solution.getValue("Context"));
//                allSubjects.add(solution.getValue("subject").stringValue());
            }
        }

        Set<Quad> retrievedQuads = new HashSet<>();
        //notify listeners of new quad
        counter += retrievedQuads.size();
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
            Set<Quad> tmpQuads = getAllQuadsForSubject(inputQuad.getObject().toString(), null);
            getAllQuadsForSubjectRecursive(tmpQuads);
        }
    }
}
