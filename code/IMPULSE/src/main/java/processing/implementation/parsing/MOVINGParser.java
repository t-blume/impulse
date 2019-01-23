package main.java.processing.implementation.parsing;

import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;
import javatools.parsers.Name;
import main.java.common.data.model.*;
import main.java.common.implementation.*;
import main.java.common.interfaces.IInstanceElement;
import main.java.common.interfaces.IQuint;
import main.java.common.interfaces.IResource;
import main.java.processing.interfaces.IElementCache;
import main.java.utils.Constants;
import main.java.utils.DataCleansing;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.RiotNotFoundException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.*;

import static main.java.utils.MainUtils.*;

/**
 * Created by Blume Till on 13.02.2017.
 */
public class MOVINGParser {
    private static final Logger logger = LogManager.getLogger(MOVINGParser.class.getSimpleName());
    private static final DataCleansing dataCleansing = new DataCleansing();


    public static HashSet<String> newData;

    private IElementCache<IInstanceElement> rdfInstanceCache;
    private Mapping mapping;

    private List<String[]> mandatoryPredicates;
    private List<String[]> mandatoryTypes;

    private int dataItemsWithMoreThanOneTitle = 0;
    private int dataItemsWithMoreThanOneAbstract = 0;
    private int dataItemCounter = 0;

    private int missingPerson;
    private int missingVenue;
    private int missingConcept;

    private Boolean downloadCacheMiss = false;


    private boolean cacheMiss = false;

    public MOVINGParser(IElementCache<IInstanceElement> rdfInstanceCache, Mapping mapping, boolean downloadCacheMiss) {
        this.rdfInstanceCache = rdfInstanceCache;
        this.mapping = mapping;
        this.downloadCacheMiss = downloadCacheMiss;

        setMissingConcept(0);
        setMissingPerson(0);
        setMissingVenue(0);


        /*
            mandatory predicates holds a list with alternative predicates. Each entry in the
            list has one or more alternatives of which one has to be met
         */


        mandatoryPredicates = new LinkedList<>();
        Set<String> mandatoryKeys = mapping.getProperties();


        mandatoryKeys.forEach(K -> {
            //check for presence of mappings from internal data format to RDF
            if (mapping.getMappings().has(K)) {
                String[] tmps = convertJSONArray2StringArray(mapping.getMappings().getJSONArray(K));
                mandatoryPredicates.add(tmps);

            }
        });


        Set<String> types = mapping.getTypes();
        String[] tmps = new String[types.size()];
        types.toArray(tmps);
        mandatoryTypes = new LinkedList<>();
        mandatoryTypes.add(tmps);

        newData = new HashSet<>();
    }




    private boolean testInstance(IInstanceElement instanceElement) {
        if (instanceElement == null)
            return false;


        //for each predicate a list of alternatives is possible
//        for (String[] predicates : mandatoryPredicates) {
//
//            boolean found = false;
//            //for each list of alternative predicates, check if at least 1 is present
//            for (IQuint quint : instanceElement.getOutgoingQuints()) {
//                for (String predicate : predicates) {
//                    if (predicate.equals(quint.getPredicate().toString())) {
//                        found = true;
//                        break;
//                    }
//                }
//            }
//            if (!found)
//                return false;
//        }


        for (String[] types : mandatoryTypes) {
            boolean found = false;
            for (IQuint quint : instanceElement.getOutgoingQuints()) {
                if (quint.getPredicate().toString().equals(Constants.RDF_TYPE)) {
                    for (String type : types) {
                        if (type.equals(quint.getObject().toString())) {
                            found = true;
                            break;
                        }
                    }
                }
            }
            if (!found)
                return false;

        }
        return true;
    }

    public DataItem convertInstance2JSON(IInstanceElement instanceElement) {

        // check if the mandatory attributes are present

        if (!testInstance(instanceElement))
            return null;


        // System.out.println(instanceElement);
        DataItem dataItem = new DataItem();
        dataItem.setSourceURLs(new HashSet<>());

        dataItem.setSource(DataItem.Source.BTC_2014);
        dataItem.setDocType(DataItem.DocType.DOCUMENT_RDF);

        int size = instanceElement.getOutgoingQuints().size();

        for (IQuint quint : instanceElement.getOutgoingQuints()) {
            //limit instance size
            if (size >= 50000)
                continue;

            size--;
//            System.out.format("\t\t\t\t\t\t\t\t\t\t\t\t\t\tOutgoingquints left: %08d \r", size);
            try {
                dataItem.getSourceURLs().add(new URI(editStrings(getContext(instanceElement))));
            } catch (URISyntaxException e) {
                logger.warn(e.getMessage());
            }
            try {

                URI property = new URI(editStrings(quint.getPredicate().toString()));
                String object = quint.getObject().toString();
                String[] alternatives;


                //TITLE:
                alternatives = convertJSONArray2StringArray(
                        mapping.getMappings().getJSONArray("title"));

                if (contains(alternatives, property.toString())) {
                    parseTitleString(dataCleansing.cleanse(object), dataItem);
                    continue;
                }
                //ABSTRACT:
                alternatives = convertJSONArray2StringArray(
                        mapping.getMappings().getJSONArray("abstract"));
                if (contains(alternatives, property.toString())) {
                    if (dataItem.getAbstract() == null) {
                        dataItem.setAbstract(dataCleansing.cleanse(object));
                    } else {
                        if (dataItem.getAbstract().length() < object.length())
                            dataItem.setAbstract(dataCleansing.cleanse(object));
                        dataItemsWithMoreThanOneAbstract++;
                    }
                    continue;
                }
                //PERSONS:
                alternatives = convertJSONArray2StringArray(
                        mapping.getMappings().getJSONArray("metadata_persons"));

                if (contains(alternatives, property.toString())) {

                    parseComplexPerson(quint, dataItem);

                    if (cacheMiss && downloadCacheMiss) {
                        requestRDFResource(quint.getObject(), quint.getContext(), instanceElement.getOutgoingQuints());
                        cacheMiss = false;
                    }

                    continue;
                }

                //CONCEPTS
                alternatives = convertJSONArray2StringArray(mapping.getMappings().getJSONArray("concepts"));
                if (contains(alternatives, property.toString())) {
                    parseComplexConcept(quint, dataItem);
                    if (cacheMiss && downloadCacheMiss) {
                        requestRDFResource(quint.getObject(), quint.getContext(), instanceElement.getOutgoingQuints());
                        cacheMiss = false;
                    }
                    continue;
                }


                //STARTDATE
                alternatives = convertJSONArray2StringArray(
                        mapping.getMappings().getJSONArray("startDate"));
                if (contains(alternatives, property.toString())) {
                    List<Date> dateList = parseDate(quint.getObject().toString());
                    if (dateList != null) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        try {
                            dataItem.setStartDate(sdf.format(dateList.get(0)));
                            if (dateList.size() > 1)
                                dataItem.setEndDate(sdf.format(dateList.get(1)));
                        } catch (IllegalArgumentException e) {
                            //TODO proper handling
                        }
                    }
                    continue;
                }
                //VENUE
                alternatives = convertJSONArray2StringArray(
                        mapping.getMappings().getJSONArray("metadata_venue"));
                if (contains(alternatives, property.toString())) {
                    parseComplexVenue(quint, dataItem);
                    if (cacheMiss && downloadCacheMiss) {
                        requestRDFResource(quint.getObject(), quint.getContext(), instanceElement.getOutgoingQuints());
                        cacheMiss = false;
                    }
                    continue;
                }

//                alternatives = convertJSONArray2StringArray(
//                        mapping.getMappings().getJSONArray("language"));
//                if (contains(alternatives, property)) {
//                    if (quint.getObject() instanceof NodeResource) {
//                        NodeResource nr = (NodeResource) quint.getObject();
//
//                        /*
//                        if (nr.getNode() instanceof RDFDataset.Literal)
//
//                        {
//                            String language = parseLanguage(quint.getObject().toString());
//                            if (language != null)
//                                dataItem.setLanguage(language);
//                        } else {
//                            //FIXME:
//                            parseLangURL(quint.getObject().toString(), dataItem);
//                        }
//*/
//
//                    }


            } catch (URISyntaxException e) {
                logger.warn(e.getMessage());
            }
        }
        dataItemCounter++;
        return dataItem;
    }

    private void requestRDFResource(IResource object, IResource quintContext, Set<IQuint> outgoingQuints) {
//TODO erroe code handling
        try {


            System.out.println("Resource to process: " + object);
            String url = object.toString();
            Model model = ModelFactory.createDefaultModel();
            //Request resource
            model.read(url);
            // model.write(System.out, "NQUAD");

            outgoingQuints.forEach(x -> addNewData(x));
            StmtIterator it = model.listStatements();

            while (it.hasNext()) {
                Statement stmt = it.next();

                //process statement to own data format
                Node[] nodes = new Node[]{new Resource(stmt.getSubject().toString()), new Resource(stmt.getPredicate().toString()), new Resource(stmt.getObject().toString()), new Resource(quintContext.toString())};

                Node subject = nodes[0];
                Node predicate = nodes[1];
                Node object2 = nodes[2];
                Node context = nodes[3];

                IQuint quint = null;

                quint = new Quad(new NodeResource(subject), new NodeResource(
                        predicate), new NodeResource(object2), new NodeResource(
                        context));

//           // outgoingQuints.add(quint);

                addNewData(quint);


            }
        } catch (RiotNotFoundException e) {
            logger.warn(e.getMessage());
        }

    }

    private void addNewData(IQuint x) {
        Node[] nodes = new Node[]{new Resource(x.getSubject().toString()), new Resource(x.getPredicate().toString()), new Resource(x.getObject().toString()), new Resource(x.getContext().toString())};
        String s = "";
        for (Node n : nodes) {

            s = s + n.toString() + " ";

        }
        s = s + ".";
        newData.add(s);
    }


    public void finished() {
        logger.info("Harvesting finished!");
        logger.info("Successfully parsed " + dataItemCounter + " bibliographic data items");
        logger.info("Data Items with more than 1 title: " + dataItemsWithMoreThanOneTitle);
        logger.info("Data Items with more than 1 abstract: " + dataItemsWithMoreThanOneAbstract);

    }

    //////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////

    private void parseComplexConcept(IQuint quint, DataItem dataItem) throws URISyntaxException {

        if (quint.getObject() instanceof NodeResource) {
            if (dataItem.getKeywords() == null)
                dataItem.setKeywords(new HashSet<>());

            if (dataItem.getConcepts() == null)
                dataItem.setConcepts(new HashSet<>());
            NodeResource nr = (NodeResource) quint.getObject();

            //First Case if object is literal add label to keywords
            // if (quint.getObject().toString().matches("\".*\"(@[a-z]+)?")) {
            if (nr.getNode() instanceof Literal) {
                Concept keywords = new Concept();
                String keywordString = quint.getObject().toString();
                keywords.setLabel(keywordString);

                String[] parts = keywordString.split(" ");
                for (int i = 0; i < parts.length; i++) {
                    dataItem.getKeywords().add(dataCleansing.cleanse(parts[i]));
                }


            } else {
                //Second Case if object is and URI get URI from InstanceCache and Parse Label
                IResource targetInstanceLocator = new TypedResource(quint.getObject(), RDFInstance.RESOURCE_TYPE);
                IInstanceElement objectElement = rdfInstanceCache.get(targetInstanceLocator);
                if (objectElement != null) {
                    Concept concept = new Concept();
                    try {
                        URI objectURI = new URI(editStrings(objectElement.getLocator().toString()));
                        concept.setURL(objectURI);
                    } catch (URISyntaxException e) {
                        logger.warn(e.getMessage());
                    }

                    for (IQuint objectQuint : objectElement.getOutgoingQuints()) {
                        URI objectProperty = new URI(editStrings(objectQuint.getPredicate().toString()));
                        String[] alternatives = convertJSONArray2StringArray(
                                mapping.getMappings().getJSONObject("metadata_concept_mapping")
                                        .getJSONArray("rawName"));
                        if (contains(alternatives, objectProperty.toString())) {
                            concept.setLabel(dataCleansing.cleanse(objectQuint.getObject().toString()));
                            continue;
                        }
                    }
                    dataItem.getConcepts().add(concept);
                } else {
                    //Third Case, Resource is not in Cache. Download Resource
                    cacheMiss = true;
                    missingConcept++;
                    //TODO
                }
            }
        }
    }

    private void parseComplexVenue(IQuint quint, DataItem dataItem) throws URISyntaxException {
        if (quint.getObject() instanceof NodeResource) {
            NodeResource nr = (NodeResource) quint.getObject();
            if (nr.getNode() instanceof Literal) {
                MetadataVenue venue = new MetadataVenue();
                parseVenueName(quint.getObject().toString(), venue);
                dataItem.setMetadataVenue(venue);
            } else {
                IResource targetInstanceLocator = new TypedResource(quint.getObject(), RDFInstance.RESOURCE_TYPE);
                IInstanceElement objectElement = rdfInstanceCache.get(targetInstanceLocator);

                if (objectElement != null) {
                    MetadataVenue venue = new MetadataVenue();

                    try {

                        //Check if Subject is a BNode
                        venue.setURIs(new HashSet<>());
                        if (editStrings(nr.getNode().toString()).startsWith("_:")) {
                            venue.getURIs().add(new URI(editStrings(quint.getContext().toString())));
                        } else {
                            URI objectURI = new URI(editStrings(objectElement.getLocator().toString()));
                            venue.getURIs().add(objectURI);
                        }
                    } catch (URISyntaxException e) {
                        logger.warn(e.getMessage());
                    }
                    for (IQuint objectQuint : objectElement.getOutgoingQuints()) {
                        URI objectProperty = new URI(editStrings(objectQuint.getPredicate().toString()));

                        String[] alternatives = convertJSONArray2StringArray(
                                mapping.getMappings().getJSONObject("metadata_venue_mapping")
                                        .getJSONArray("rawName"));
                        if (contains(alternatives, objectProperty.toString())) {
                            parseVenueName(objectQuint.getObject().toString(), venue);
                            continue;
                        }
                        alternatives = convertJSONArray2StringArray(
                                mapping.getMappings().getJSONObject("metadata_venue_mapping")
                                        .getJSONArray("volume"));
                        if (contains(alternatives, objectProperty.toString())) {
                            parseVolume(quint.getObject().toString(), dataItem.getMetadataVenue());
                            continue;
                        }
                        alternatives = convertJSONArray2StringArray(
                                mapping.getMappings().getJSONObject("metadata_venue_mapping")
                                        .getJSONArray("location"));

                        if (contains(alternatives, objectProperty.toString())) {
                            parseLocation(quint.getObject().toString(), dataItem.getMetadataVenue());
                        }
                    }
                    dataItem.setMetadataVenue(venue);
                } else {
                    //TODO: handle download
                    cacheMiss = true;
                    missingVenue++;
                }
            }
        }
    }


    private void parseComplexPerson(IQuint quint, DataItem dataItem) throws URISyntaxException {

        HashSet<MetadataPerson> metadataPeople = new HashSet<>();

        if (quint.getObject() instanceof NodeResource) {

            NodeResource nr = (NodeResource) quint.getObject();
            //Simple Case: links to a literal:
            if (nr.getNode() instanceof Literal) {
                Set<MetadataPerson> personSet = parseAuthorNames(quint, quint.getObject().toString());
                if (dataItem.getMetadataPersons() == null)
                    dataItem.setMetadataPersons(new HashSet<>());

                // add authors, contributors
                dataItem.getMetadataPersons().addAll(personSet);

                //Blanknode handling
            } else if (nr.getNode() instanceof BNode) {

                IResource targetInstanceLocator = new TypedResource(quint.getObject(), RDFInstance.RESOURCE_TYPE);
                IInstanceElement objectElement = rdfInstanceCache.get(targetInstanceLocator);
                boolean organisation = quint.getObject().toString().contains("organization");

                try {
                    rdfInstanceCache.get(objectElement.getLocator()).getOutgoingQuints().forEach(x -> {
                        if (x.getObject() instanceof NodeResource) {
                            IResource targetInstanceLocator2 = new TypedResource(x.getObject(), RDFInstance.RESOURCE_TYPE);
                            IInstanceElement objectElement2 = rdfInstanceCache.get(targetInstanceLocator2);
                            if (objectElement2 != null) {
                                for (IQuint objectQuint : objectElement2.getOutgoingQuints()) {
                                    MetadataPerson metadataPerson = extractPersons(objectQuint, organisation, dataItem);
                                    if (metadataPerson != null)
                                        metadataPeople.add(metadataPerson);
                                }
                            }
                        }
                    });
                } catch (NullPointerException e) {
                    e.printStackTrace();
                }
                //%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
            } else {
                //complex case, the object is again a URI with properties etc.
                IResource targetInstanceLocator = new TypedResource(quint.getObject(), RDFInstance.RESOURCE_TYPE);
                IInstanceElement objectElement = rdfInstanceCache.get(targetInstanceLocator);

                if (objectElement != null) {
                    //FIXME: quick hax
                    boolean organisation = quint.getObject().toString().contains("organization");
                    MetadataPerson metadataPerson = new MetadataPerson();
                    MetadataOrganisation metadataOrganisation = new MetadataOrganisation();
                    try {
                        URI objectURI = new URI(editStrings(objectElement.getLocator().toString()));
                        boolean found = false;

                        if (dataItem.getMetadataOrganisations() != null) {
                            for (MetadataOrganisation prevOrga : dataItem.getMetadataOrganisations()) {
                                if (prevOrga.getURIs() != null && prevOrga.getURIs().contains(objectURI)) {
                                    metadataOrganisation = prevOrga;
                                    found = true;
                                    break;
                                }
                            }
                        }
                        //Check if subject is a BNode
                        metadataOrganisation.setURIs(new HashSet<>());
                        if (!found) {
                            if (editStrings(nr.getNode().toString()).startsWith("_:")) {
                                metadataOrganisation.getURIs().add(new URI(editStrings(quint.getContext().toString())));
                            } else {
                                metadataOrganisation.getURIs().add(objectURI);
                            }
                        }
                        found = false;
                        if (dataItem.getMetadataPersons() != null) {
                            for (MetadataPerson prevPers : dataItem.getMetadataPersons()) {
                                if (prevPers.getURIs() != null && prevPers.getURIs().contains(objectURI)) {
                                    metadataPerson = prevPers;
                                    found = true;
                                    break;
                                }
                            }
                        }
                        if (!found) {

                            //Check if subject is a BNode
                            metadataPerson.setURIs(new HashSet<>());
                            if (editStrings(nr.getNode().toString()).startsWith("_:")) {
                                metadataPerson.getURIs().add(new URI(editStrings(quint.getContext().toString())));
                            } else {
                                metadataPerson.getURIs().add(objectURI);
                            }
                        }
                    } catch (URISyntaxException e) {
                        logger.warn(e.getMessage());
                    }

                    for (IQuint objectQuint : objectElement.getOutgoingQuints()) {
                        //Check if Quintobject is an URI
                        if (objectQuint.getObject().toString().startsWith("http")) {

                            IResource targetInstanceLocator2 = new TypedResource(objectQuint.getObject(), RDFInstance.RESOURCE_TYPE);
                            IInstanceElement objectElement2 = rdfInstanceCache.get(targetInstanceLocator2);
                            if (objectElement2 != null) {
                                for (IQuint objectQuint2 : objectElement2.getOutgoingQuints()) {

                                    metadataPerson = extractPersons(objectQuint2, false, dataItem);
                                    if (metadataPerson != null)
                                        metadataPeople.add(metadataPerson);
                                }
                            }
                        } else {
                            metadataPerson = extractPersons(objectQuint, organisation, dataItem);
                            if (metadataPerson != null)
                                metadataPeople.add(metadataPerson);
                        }


                    }
                } else {
                    //TODO: what to do here? ...
                    //If Person is not in Cache download person data
                    cacheMiss = true;
                    missingPerson++;
                }
            }
        }
        dataItem.setMetadataPersons(metadataPeople);
    }

    //////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////

    private MetadataPerson extractPersons(IQuint objectQuint, boolean organisation, DataItem dataItem) {

        MetadataPerson metadataPerson = new MetadataPerson();
        MetadataOrganisation metadataOrganisation = new MetadataOrganisation();

        if (metadataOrganisation.getRoles() == null)
            metadataOrganisation.setRoles(new HashSet<>());
        if (metadataPerson.getRoles() == null)
            metadataPerson.setRoles(new HashSet<>());

        HashSet<String> roles = new HashSet<>();
        //based on the link-label, determine the role of that person
        roles.add(parseSimpleAuthorRole(objectQuint));

        metadataPerson.getRoles().addAll(roles);
        metadataOrganisation.getRoles().addAll(roles);

        try {
            URI objectProperty = new URI(editStrings(objectQuint.getPredicate().toString()));

            String[] alternatives = convertJSONArray2StringArray(
                    mapping.getMappings().getJSONObject("metadata_person_mapping").getJSONArray("rawName"));
            if (contains(alternatives, objectProperty.toString())) {

                metadataPerson.setRawName(dataCleansing.cleanse(objectQuint.getObject().toString()));
                metadataOrganisation.setName(objectQuint.getObject().toString());

                if (!organisation) {

                    String[] strings = objectQuint.getObject().toString().split(" and ");

                    for (String t : strings) {
                        return parseAuthorString(dataCleansing.cleanse(t), metadataPerson);
                    }
                } else {
                    //if creator is an organization
                    MetadataPerson metadataOrg = new MetadataPerson();

                    metadataOrg.setRawName(metadataOrganisation.getName());
                    metadataOrg.setURIs(new HashSet<>());

                    //Check if subject is a BNode
                    if (editStrings(objectQuint.getSubject().toString()).startsWith("_:")) {
                        metadataOrg.getURIs().add(new URI(editStrings(objectQuint.getContext().toString())));
                    } else {
                        metadataOrg.getURIs().add(new URI(objectQuint.getSubject().toString()));

                    }

                    metadataOrg.setRoles(new HashSet<>());
                    metadataOrg.getRoles().addAll(roles);
                    return metadataOrg;
                }
            }
        } catch (Exception e) {
            //todo
        }
        return null;
    }

    private String parseSimpleAuthorRole(IQuint quint) {
        //FIXME: quick hax
        if (quint.getPredicate().toString().contains("creator") || quint.getPredicate().toString().contains("author"))
            return "author";
        else if (quint.getPredicate().toString().contains("contributor"))
            return "contributor";
        else if (quint.getPredicate().toString().contains("organization"))
            return "organization";
        else return "author"; //Proper default?
    }

    private Set<MetadataPerson> parseAuthorNames(IQuint quint, String rawName) {
        Set<MetadataPerson> authors = new HashSet<>();

        HashSet<String> roles = new HashSet<>();
        //based on the link-label, determine the role of that person
        roles.add(parseSimpleAuthorRole(quint));

        //TODO: optimize regex pattern
        String[] strings = rawName.split(" *(and|AND|,) +");
        for (String s : strings) {
            //  System.out.println("AuthorString: " + s);
            MetadataPerson metadataPerson = new MetadataPerson();
            metadataPerson.setRoles(roles);
            parseAuthorString((dataCleansing.cleanse(s)), metadataPerson);
            authors.add(metadataPerson);
        }
        return authors;
    }

    //PARSE KRAM
    private static void parseLangURL(String langURL, DataItem bibliographicDataItem) {
        //ex: http://lexvo.org/id/iso639-3/eng
        if (langURL == null)
            return;
        if (langURL.contains("iso639")) {
            String[] split = langURL.split("/|#");
            if (split.length > 0) {
                if (split[split.length - 1].length() <= 0) {
                    if (split.length - 2 > 0)
                        bibliographicDataItem.setLanguage(parseLanguage(split[split.length - 2]));
                } else
                    bibliographicDataItem.setLanguage(parseLanguage(split[split.length - 1]));
            }
        }
    }

    private static void parseVenueName(String rawText, MetadataVenue venue) {
        if (rawText == null || venue == null)
            return;
        //TODO: proper parsing
        rawText = rawText.replaceAll("\"", "");
        rawText = rawText.trim();
        venue.setRawName(rawText);
    }

    private static void parseVolume(String rawText, MetadataVenue venue) {
        // <http://purl.org/ontology/bibo/volume>
        rawText = rawText.replaceAll("\"", "");
        rawText = rawText.trim();
        try {
            venue.setVolume(Integer.parseInt(rawText));
        } catch (NumberFormatException e) {
            logger.warn(e.getMessage());
            e.printStackTrace();
        }
    }

    private static void parseLocation(String rawText, MetadataVenue venue) {
        if (isNode(rawText)) {
            //TODO:
        } else {
            rawText = rawText.replaceAll("\"", "");
            rawText = rawText.trim();
            Location location = new Location();
            location.setName(rawText);
            if (venue.getLocation() == null)
                venue.setLocation(location);
        }

    }

    private static boolean isNode(String input) {
        return input.matches("http://.*|_:.*");
    }

    private void parseTitleString(String rawTitle, DataItem dataItem) {
        if (rawTitle == null || dataItem == null)
            return;

        //TODO: proper parsing
        if (rawTitle.matches(".*\"@[a-z]+")) {
            String[] split = rawTitle.split("@");
            dataItem.setLanguage(parseLanguage(split[split.length - 1]));
            String tmpTitle = "";
            for (int i = 0; i < split.length - 1; i++)
                tmpTitle += split[i];

            rawTitle = tmpTitle;
        }
        rawTitle = rawTitle.replaceAll("\"", "");
        rawTitle = rawTitle.trim();


        if (dataItem.getTitle() == null) {
            dataItem.setTitle(rawTitle);
        } else {
            if (rawTitle.length() > dataItem.getTitle().length())
                dataItem.setTitle(rawTitle);

            dataItemsWithMoreThanOneTitle++;
        }
    }


    private static Map<String, Locale> localeMap;

    private static Map<String, Locale> getLanguageCodeMapping() {
        if (localeMap != null)
            return localeMap;

        String[] languages = Locale.getISOLanguages();
        localeMap = new HashMap<>(languages.length);
        for (String language : languages) {
            Locale locale = new Locale(language, "");
            localeMap.put(locale.getISO3Language().toUpperCase(), locale);
        }
        return localeMap;
    }

    private static String iso3CountryCodeToIso2CountryCode(String iso3LanguageCode) {
        if (getLanguageCodeMapping().containsKey(iso3LanguageCode.toUpperCase()))
            return getLanguageCodeMapping().get(iso3LanguageCode.toUpperCase()).getLanguage();
        else return null;
    }

    private static String parseLanguage(String language) {
        language = language.trim();
        if (language.matches("[a-z]{2}"))
            return language;
        else if (language.matches("[a-z]{3}"))
            return iso3CountryCodeToIso2CountryCode(language);
        else
            return null; //TODO: more parsing options
    }

    private static List<Date> parseDate(String rawDate) {
        rawDate = rawDate.replaceAll("\"", "");
        rawDate = rawDate.trim();
        //FIXME: known bugs of the parser
        if (rawDate.matches("[0-9]{4}"))
            rawDate += "-01-01";
        else if (rawDate.matches("[0-9]{4}-[0-1][0-9]"))
            rawDate += "-01";
        else if (rawDate.matches("[0-9]+-[0-9]+"))
            return null;

        Parser parser = new Parser();
        List<DateGroup> groups = parser.parse(rawDate);
        if (groups.size() > 0) {
            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.DATE, -1);
            if (groups.get(0).getDates().get(0).before(cal.getTime()))
                return groups.get(0).getDates();
            else
                return null;
        } else
            return null;
    }


    private static String toDisplayCase(String s) {
        final String ACTIONABLE_DELIMITERS = " '-/"; // these cause the character following
        // to be capitalized

        StringBuilder sb = new StringBuilder();
        boolean capNext = true;

        for (char c : s.toCharArray()) {
            c = (capNext)
                    ? Character.toUpperCase(c)
                    : Character.toLowerCase(c);
            sb.append(c);
            capNext = (ACTIONABLE_DELIMITERS.indexOf((int) c) >= 0); // explicit cast not needed
        }
        return sb.toString();
    }

    private static MetadataPerson parseAuthorString(String nameString, MetadataPerson author) {

        if (nameString == null || author == null)
            return null;
        author.setRawName(nameString);
        // System.out.println(nameString);

        //preprocessing!
        nameString = nameString.replaceAll("\"", "");
        nameString = nameString.replaceAll("(, )?[0-9]{2,4}-([0-9]{2,4})?", "");
        nameString = nameString.replaceAll("\\(.*\\)", "");
        nameString = toDisplayCase(nameString);
        //nameString = WordUtils.capitalizeFully(nameString);
        //System.out.println(nameString);

        if (nameString.contains(",")) {
            String[] split = nameString.split(",");
            for (int i = 0; i < split.length - 1; i++) {
                nameString = split[i + 1] + " " + split[i];
            }
            if (split.length % 2 != 0) {
                //odd number of splits for some reason
                nameString += " " + split[split.length - 1];
            }
        }
        nameString = nameString.trim();

        Name.PersonName personName = new Name.PersonName(nameString);

        String normName;
        String initials = "";
        if (personName.givenNames() != null) {
            String[] names = personName.givenNames().split(" ");
            for (String name : names) {
                if (!name.isEmpty())
                    initials += name.charAt(0) + ". ";
            }

        } else
            return author;
        if (personName.familyName() != null) {
            normName = personName.familyName() + ", " + initials.trim();
        } else
            return author;
        author.setName(normName);
        return author;

    }

    private static String getContext(IInstanceElement instanceElement) {
        HashMap<String, Integer> contexts = new HashMap<>();
        if (instanceElement == null)
            return null;
        for (IQuint quint : instanceElement.getOutgoingQuints())
            contexts.merge(quint.getContext().toString(), 1, (OLD, NEW) -> OLD + NEW);

        String maxK = null;
        Integer maxV = -1;
        for (Map.Entry<String, Integer> entry : contexts.entrySet()) {
            if (entry.getValue() > maxV) {
                maxV = entry.getValue();
                maxK = entry.getKey();
            }
        }
        //System.out.println("maxK: " + maxK);
        return maxK;
    }

    public int getMissingPerson() {
        return missingPerson;
    }

    public void setMissingPerson(int missingPerson) {
        this.missingPerson = missingPerson;
    }

    public int getMissingVenue() {
        return missingVenue;
    }

    public void setMissingVenue(int missingVenue) {
        this.missingVenue = missingVenue;
    }

    public int getMissingConcept() {
        return missingConcept;
    }

    public void setMissingConcept(int missingConcept) {
        this.missingConcept = missingConcept;
    }
}
