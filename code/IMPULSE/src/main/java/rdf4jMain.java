package main.java;

import main.java.common.interfaces.IResource;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

public class rdf4jMain {

    public static void main(String[] args) throws IOException {

        MyTripleStore ts = new MyTripleStore();
        List<String> inputFiles = new LinkedList<>();
        inputFiles.add("sample.nt");
        String regexFileFilter = ".*";

        final String finalRegex = regexFileFilter;
        FileFilter fileFilter = (pathname) -> (pathname != null ? pathname.toString().matches(finalRegex) : false);

        FileQuadSource quintSource = new FileQuadSource(inputFiles, true,
                "http://harverster.informatik.uni-kiel.de/", fileFilter);

        quintSource.start();

//        System.out.println("hier" + ts.getQuints("http://dbpedia.org/resource/Bob_Marley"));


//        ts.loadSubjects();
//
//        ts.getAllQuadsForSubject("http://dbpedia.org/resource/Bob_Marley");

//        ts.loadZippedFile(new FileInputStream("sample-rdf-data.nt"), RDFFormat.TURTLE);

//


    }


}
