package main.java.utils;

import java.util.HashSet;
import java.util.Set;

/**
 * Static class for various constants used
 *
 * @author Bastian
 */
public class Constants {


    /**
     * URL of the owl:SameAs property
     */
    public final static String OWL_SameAs = "http://www.w3.org/2002/07/owl#sameAs";

    // RDF(S) vocabulary

    /**
     * URL of the RDF type property
     */
    public final static String RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    /**
     * URL of the RDF label property
     */
    public final static String RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label";
    /**
     * URL of the RDFS literal type
     */
    public final static String RDFS_LITERAL = "http://www.w3.org/2000/01/rdf-schema#Literal";

    /**
     * URL of the RDFS subClassOf property
     */
    public final static String RDFS_SUBCLASSOF = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
    /**
     * URL of the RDFS subPropertyOf property
     */
    public final static String RDFS_SUBPROPERTYOF = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf";
    /**
     * URL of the RDFS domain property
     */
    public final static String RDFS_DOMAIN = "http://www.w3.org/2000/01/rdf-schema#domain";

    /**
     * URL of the RDFS range property
     */
    public final static String RDFS_RANGE = "http://www.w3.org/2000/01/rdf-schema#range";

    /**
     * Set containing the RDFS properties
     */
    public static final Set<String> RDFS_PROPERTIES;

    static {
        RDFS_PROPERTIES = new HashSet<>();
        RDFS_PROPERTIES.add(RDFS_SUBCLASSOF);
        RDFS_PROPERTIES.add(RDFS_SUBPROPERTYOF);
        RDFS_PROPERTIES.add(RDFS_DOMAIN);
        RDFS_PROPERTIES.add(RDFS_RANGE);
    }

    /**
     * Set containing properties associated with some kind of label
     */
    public static final Set<String> SNIPPET_PROPERTIES;

    static {
        SNIPPET_PROPERTIES = new HashSet<>();
        SNIPPET_PROPERTIES.add(RDFS_LABEL);
        SNIPPET_PROPERTIES.add("http://www.w3.org/2004/02/skos/core#altLabel");
        SNIPPET_PROPERTIES.add("http://www.w3.org/2004/02/skos/core#prefLabel");
        SNIPPET_PROPERTIES.add("http://www.w3.org/TR/rdf-schema/#ch_comment");
        SNIPPET_PROPERTIES.add("http://purl.org/dc/terms/title");
        SNIPPET_PROPERTIES
                .add("http://dublincore.org/2010/10/11/dcterms.rdf#title");
    }


}
