package main.java.output.implementation;

import main.java.common.interfaces.IQuint;
import main.java.input.implementation.RDF4JQuadSource;
import main.java.output.interfaces.IQuadSink;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class RDF4QuadSink implements IQuadSink {

    private static final Logger logger = LogManager.getLogger(RDF4QuadSink.class.getSimpleName());


    private final int BUFF_SIZE = 10000;
    private final int MAX_STRING_LENGTH = 1000;
    private final StringBuffer sbf = new StringBuffer(BUFF_SIZE * MAX_STRING_LENGTH);

    private long addCount = 0;
    private long min = BUFF_SIZE * MAX_STRING_LENGTH;

    public Repository repo;
    public RepositoryConnection conn;

    public RDF4QuadSink(String URL, String ID) {
        logger.debug("Opening Connection to " +  URL + " (" + ID + ")");
        this.repo = new HTTPRepository(URL, ID);
        repo.init();
        this.conn = repo.getConnection();
    }

    @Override
    public boolean addQuint(IQuint quint) {
        return addStream(quint.getSubject().toN3(), quint.getPredicate().toN3(), quint.getObject().toN3());

    }

    @Override
    public void finished() {
        logger.info("Closing connection..." );
        conn.close();
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
                logger.error("printTriple() Exception thrown  :" + e);
                logger.debug(sbf.toString());
                System.exit(-1);
            }
            // reset stream
            sbf.setLength(0);
        }
        return true;
    }
}
