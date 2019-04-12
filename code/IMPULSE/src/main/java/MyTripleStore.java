package main.java;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import main.java.common.implementation.NodeResource;
import main.java.common.implementation.Quad;
import main.java.common.interfaces.IQuint;
import main.java.common.interfaces.IResource;
import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.event.base.NotifyingRepositoryConnectionWrapper;
import org.eclipse.rdf4j.repository.event.base.RepositoryConnectionListenerAdapter;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.util.RDFInserter;
import org.eclipse.rdf4j.repository.util.RDFLoader;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.semanticweb.yars.nx.Resource;

public class MyTripleStore {
    private final int BUFF_SIZE = 10000;
    private final int MAX_STRING_LENGTH = 1000;
    private final StringBuffer sbf = new StringBuffer(BUFF_SIZE * MAX_STRING_LENGTH);

    private long addCount = 0;
    private long min = BUFF_SIZE * MAX_STRING_LENGTH;

    public Repository repo;
    public RepositoryConnection conn;

    /**
     * Creates an inmemory triple store
     */
    public MyTripleStore() {

        String rdf4jServer = "http://localhost:8080/rdf4j-server/";
        String repositoryID = "02";
        this.repo = new HTTPRepository(rdf4jServer, repositoryID);

        repo.init();
        this.conn = repo.getConnection();
    }

    public void loadSubjects() {


        String queryString = "SELECT ?subject \n";
        queryString += "WHERE {?subject ?predicate ?object} \n";

        TupleQuery query = conn.prepareTupleQuery(queryString);

        // A QueryResult is also an AutoCloseable resource, so make sure it gets closed when done.
        try (
                TupleQueryResult result = query.evaluate()) {
            // we just iterate over all solutions in the result...
            while (result.hasNext()) {
                BindingSet solution = result.next();
                // ... and print out the value of the variable binding for ?s and ?n
                System.out.println(solution.getValue("subject"));
//                System.out.println("?n = " + solution.getValue("n"));
            }
        }

    }

    public void getAllQuadsForSubject(String subjectURI) {
        //TODO retrieve all quads
        RepositoryConnection conn = repo.getConnection();

        String queryString = "SELECT ?subject \n";
        queryString += "WHERE {?subject HAS ?" + subjectURI + "} \n";


        TupleQuery query = conn.prepareTupleQuery(queryString);

        // A QueryResult is also an AutoCloseable resource, so make sure it gets closed when done.
        try (TupleQueryResult result = query.evaluate()) {
            // we just iterate over all solutions in the result...
            while (result.hasNext()) {
                BindingSet solution = result.next();
                // ... and print out the value of the variable binding for ?s and ?n
//                System.out.println("Subject = " + solution.getValue("subject") + " Predicate = " + solution.getValue("predicate") + " Object = " + solution.getValue("Object") + " Context = " + solution.getValue("Context"));
                System.out.println("Subject = " + solution.getValue("subject"));
//                allSubjects.add(solution.getValue("subject").stringValue());
            }
        }
    }



    public List<IQuint> getQuints(String subjectURI) {
        IRI subject = repo.getValueFactory().createIRI(subjectURI);

        List<IQuint> quints = null;
        RepositoryResult<Statement> statements = conn.getStatements(subject, null, null);
        if (statements != null) {
            quints = new LinkedList<>();
            while (statements.hasNext()) {
                Statement statement = statements.next();
                quints.add(new Quad(new NodeResource(new org.semanticweb.yars.nx.Resource(statement.getSubject().stringValue())),
                        new NodeResource(new org.semanticweb.yars.nx.Resource(statement.getPredicate().stringValue())),
                        new NodeResource(new Resource(statement.getObject().stringValue()))));
            }
        }
        return quints;
    }



    public boolean addQuint(IQuint quint) {
        return addStream(quint.getSubject().toN3(), quint.getPredicate().toN3(), quint.getObject().toN3());
    }

    public boolean addQuints(List<IQuint> quints) {
        return addStream(quints);
    }


    private boolean addStream(List<IQuint> quints) {
        StringBuilder sb = new StringBuilder();
        for (IQuint quint : quints)
            sb.append(quint.getSubject().toN3().trim() + quint.getPredicate().toN3().trim() +
                     quint.getObject().toN3().trim() + ".\n");
        sbf.append(sb);

        int len = BUFF_SIZE * MAX_STRING_LENGTH - sbf.length();
        min = Math.min(min, len);
        if (addCount % BUFF_SIZE == 0) {
            InputStream in = new ByteArrayInputStream(sbf.toString().getBytes());
            try {
                conn.begin();
                conn.add(in, null, RDFFormat.NTRIPLES);
                conn.commit();
            } catch (RepositoryException | IOException e) {
                System.out.println("printTriple() Exception thrown  :" + e);
                System.out.println(sbf.toString());
                System.exit(-1);
            }
            // reset stream
            sbf.setLength(0);
        }
        return true;
    }


    private boolean addStream(String subject, String predicate, String object) {
        StringBuilder sb = new StringBuilder();
        sb.append(subject + " " + " " + predicate + " " + object + " .\n");
        sbf.append(sb);
        int len = BUFF_SIZE * MAX_STRING_LENGTH - sbf.length();
        min = Math.min(min, len);
        if (addCount % BUFF_SIZE == 0) {
            InputStream in = new ByteArrayInputStream(sbf.toString().getBytes());
            try {
                conn.begin();
                conn.add(in, null, RDFFormat.NTRIPLES);
                conn.commit();
            } catch (RepositoryException | IOException e) {
                System.out.println("printTriple() Exception thrown  :" + e);
                System.out.println(sbf.toString());
                System.exit(-1);
            }
            // reset stream
            sbf.setLength(0);
        }
        return true;
    }


    /**
     * @param in     gzip compressed data on an inputstream
     * @param format the format of the streamed data
     */
    public void loadZippedFile(InputStream in, RDFFormat format) {


        try (RepositoryConnection con = repo.getConnection()) {
            MyRdfInserter inserter = new MyRdfInserter(con);
            RDFLoader loader = new RDFLoader(con.getParserConfig(), con.getValueFactory());


            loader.load(in, "", format, inserter);
//        } catch (RuntimeException e) {
//           e.printStackTrace();
        } catch (RDFParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    class MyRdfInserter extends AbstractRDFHandler {
        RDFInserter rdfInserter;
        int count = 0;

        public MyRdfInserter(RepositoryConnection con) {
            rdfInserter = new RDFInserter(con);
        }

        @Override
        public void handleStatement(Statement st) {
            count++;
            if (count % 100000 == 0)
                System.out.println("Add statement number " + count + "\n"
                        + st.getSubject().stringValue() + " "
                        + st.getPredicate().stringValue() + " "
                        + st.getObject().stringValue());
            rdfInserter.handleStatement(st);
        }
    }
}
