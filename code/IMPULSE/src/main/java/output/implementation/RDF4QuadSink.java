package main.java.output.implementation;

import main.java.common.interfaces.IQuint;
import main.java.output.interfaces.IQuadSink;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPRepository;

import java.util.ArrayList;
import java.util.List;

public class RDF4QuadSink implements IQuadSink {

    private static final Logger logger = LogManager.getLogger(RDF4QuadSink.class.getSimpleName());
    private static List<Statement> stmts = new ArrayList<>();

    private final int BUFF_SIZE = 10000;
    private final int MAX_STRING_LENGTH = 1000;
    private final StringBuffer sbf = new StringBuffer(BUFF_SIZE * MAX_STRING_LENGTH);

    private long addCount = 0;
    private long min = BUFF_SIZE * MAX_STRING_LENGTH;

    public Repository repo;
    public static RepositoryConnection conn;

    public RDF4QuadSink(String URL, String ID) {
        logger.debug("Opening Connection to " + URL + " (" + ID + ")");
        this.repo = new HTTPRepository(URL, ID);
        repo.init();
        this.conn = repo.getConnection();
    }

    @Override
    public int addQuint(IQuint quint) {
        return addStream(quint.getSubject().toN3(), quint.getPredicate().toN3(), quint.getObject().toN3(), quint.getContext().toN3());

    }

    @Override
    public int finished() {
        logger.info("Closing connection...");
        logger.info("Uploading remaining stuff: " + stmts.size());
        if(addStmtToConnection(stmts)) {
            conn.close();
            return stmts.size();
        }
        else {
            conn.close();
            return -stmts.size();
        }
    }


    private int addStream(String subject, String predicate, String object, String context) {
        ValidatingValueFactory factory = new ValidatingValueFactory();
        Statement stmt = null;

        subject = subject.replace("<", "");
        subject = subject.replace(">", "");

        predicate = predicate.replace("<", "");
        predicate = predicate.replace(">", "");

        context = context.replace("<", "");
        context = context.replace(">", "");

        IRI s = factory.createIRI(subject);
        IRI p = factory.createIRI(predicate);
        IRI c = factory.createIRI(context);

        //Literal Handling
        if (!object.matches("\".*\"(@[a-z]+)?")) {
            if (object.contains("^^http")) {
                object = object.replace("^^", "~");
                String[] string = object.split("~");
                String literal = string[0];
                String value = string[1];

                literal = literal.replace("\"", "");
                literal = literal.replace("\"", "");
                org.eclipse.rdf4j.model.Literal o = factory.createLiteral(literal, value);
                stmt = factory.createStatement(s, p, o, c);
            } else {

                object = object.replace("<", "");
                object = object.replace(">", "");

                IRI o = factory.createIRI(object);
                stmt = factory.createStatement(s, p, o, c);
            }
        } else {
            object = object.replace("\"", "");
            object = object.replace("\"", "");
            org.eclipse.rdf4j.model.Literal o = factory.createLiteral(object);
            stmt = factory.createStatement(s, p, o, c);

        }


        stmts.add(stmt);
        int bulkSize = 100000;
        if (stmts.size() >= bulkSize) {
            if(addStmtToConnection(stmts)){
                stmts = new ArrayList<>();
                return bulkSize;
            }else {
                stmts = new ArrayList<>();
                return -bulkSize;
            }
        }
        return 0;
    }

    private static Boolean addStmtToConnection(List<Statement> stmts) {
            try {
                conn.begin();
                conn.add(stmts);
                conn.commit();
            } catch (Exception e) {
                logger.error("printTriple() Exception thrown  :" + e);
                conn.rollback();
                return false;
            }
        return true;
    }
}