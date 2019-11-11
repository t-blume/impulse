package main.java.processing.implementation.parsing;

import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;
import javatools.parsers.Name;
import main.java.common.data.model.*;
import main.java.common.implementation.Mapping;
import main.java.common.implementation.NodeResource;
import main.java.common.implementation.RDFInstance;
import main.java.common.implementation.TypedResource;
import main.java.common.interfaces.IInstanceElement;
import main.java.common.interfaces.IQuint;
import main.java.common.interfaces.IResource;
import main.java.processing.implementation.Harvester;
import main.java.processing.interfaces.IElementCache;
import main.java.utils.DataCleansing;
import main.java.utils.MainUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;

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

    private Harvester harvester;
    private Mapping mapping;

    private List<String[]> mandatoryPredicates;
    private List<String[]> mandatoryTypes;

    private int dataItemsWithMoreThanOneTitle = 0;
    private int dataItemsWithMoreThanOneAbstract = 0;
    private int dataItemCounter = 0;

    private int missingPerson;
    private int missingVenue;
    private int missingConcept;


    public MOVINGParser(Mapping mapping) {
        this.mapping = mapping;

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

    public void setHarvester(Harvester harvester){
        this.harvester = harvester;
    }

    public DataItem convertInstance2JSON(IInstanceElement instanceElement) {
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
                    continue;
                }

                //CONCEPTS
                alternatives = convertJSONArray2StringArray(mapping.getMappings().getJSONArray("concepts"));
                if (contains(alternatives, property.toString())) {
                    parseComplexConcept(quint, dataItem);
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
                    continue;
                }
            } catch (URISyntaxException e) {
                logger.warn(e.getMessage());
            }
        }
        dataItemCounter++;
        return dataItem;
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
                for (int i = 0; i < parts.length; i++)
                    dataItem.getKeywords().add(dataCleansing.cleanse(parts[i]));
            } else {
                //Second Case if object is and URI get URI from InstanceCache and Parse Label
                IResource targetInstanceLocator = new TypedResource(quint.getObject(), RDFInstance.RESOURCE_TYPE);
                IInstanceElement objectElement = harvester.getInstance(targetInstanceLocator);
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
                    missingConcept++;
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
                IInstanceElement objectElement = harvester.getInstance(targetInstanceLocator);

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
                    missingVenue++;
                }
            }
        }
    }


    private void parseComplexPerson(IQuint quint, DataItem dataItem) {
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
                IInstanceElement objectElement = harvester.getInstance(targetInstanceLocator);
                boolean organisation = quint.getObject().toString().contains("organization");
                try {
                    harvester.getInstance(objectElement.getLocator()).getOutgoingQuints().forEach(x -> {
                        if (x.getObject() instanceof NodeResource) {
                            IResource targetInstanceLocator2 = new TypedResource(x.getObject(), RDFInstance.RESOURCE_TYPE);
                            IInstanceElement objectElement2 = harvester.getInstance(targetInstanceLocator2);
                            if (objectElement2 != null) {
                                for (IQuint objectQuint : objectElement2.getOutgoingQuints()) {
                                    MetadataPerson metadataPerson = extractPersons(objectQuint, organisation);
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
                IInstanceElement objectElement = harvester.getInstance(targetInstanceLocator);
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
                            IInstanceElement objectElement2 = harvester.getInstance(targetInstanceLocator2);
                            if (objectElement2 != null) {
                                for (IQuint objectQuint2 : objectElement2.getOutgoingQuints()) {

                                    metadataPerson = extractPersons(objectQuint2, false);
                                    if (metadataPerson != null)
                                        metadataPeople.add(metadataPerson);
                                }
                            }
                        } else {
                            metadataPerson = extractPersons(objectQuint, organisation);
                            if (metadataPerson != null)
                                metadataPeople.add(metadataPerson);
                        }
                    }
                } else {
                    missingPerson++;
                }
            }
        }
        dataItem.setMetadataPersons(metadataPeople);
    }

    //////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////

    private MetadataPerson extractPersons(IQuint objectQuint, boolean organisation) {
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
                    for (String t : strings)
                        return parseAuthorString(dataCleansing.cleanse(t), metadataPerson);

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
        //fixes known bugs of the parser
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
//            contexts.merge(quint.getSubject().toString(), 1, (OLD, NEW) -> OLD + NEW);
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
