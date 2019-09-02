package main.java.utils;

import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.eclipse.rdf4j.repository.util.RDFInserter;
import org.eclipse.rdf4j.repository.util.RDFLoader;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.File;
import java.io.IOException;

public class RDF4JLoader {
    public static void main(String[] args) throws IOException {

        String repositoryURL = "localhost:8080/rdf4j-server";
        String repositoryID = "sample-data";

        HTTPRepository repository = new HTTPRepository(repositoryURL, repositoryID);
        RepositoryConnection connection = repository.getConnection();

        RDFInserter inserter = new RDFInserter(connection);
        RDFLoader loader = new RDFLoader(connection.getParserConfig(), connection.getValueFactory());

        loader.load(new File("testresources/sample-rdf-data.nt.gz"), "http://harverster.informatik.uni-kiel.de/", RDFFormat.NTRIPLES, inserter);

    }
}
