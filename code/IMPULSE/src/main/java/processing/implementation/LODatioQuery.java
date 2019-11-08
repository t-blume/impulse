package main.java.processing.implementation;

import main.java.common.implementation.Mapping;
import org.json.JSONObject;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLEncoder;
import java.util.*;

import static main.java.utils.MainUtils.convertJSONArray2StringList;
import static main.java.utils.MainUtils.readFile;
import static main.java.utils.WebIO.getContent;

/**
 * Created by Blume Till on 24.02.2017.
 */
public class LODatioQuery {


    public static final String _QueryInferencing = "http://lodatio.informatik.uni-kiel.de:8082/LodatioLogic/QueryInferencing?";
    public static final String _MultiMapper = "http://lodatio.informatik.uni-kiel.de:8082/LodatioLogic/MultiMapper?";

    private static List<String> attributes;
    private static Set<String> type;
    private static Set<String> query;

    private static Map map = new HashMap();
    private static Map map_mapping = new HashMap();
    private static Map map_mapping_person = new HashMap();
    private static Map map_mapping_concept = new HashMap();
    private static JSONObject inferencedMapping;


    /**
     * limit < 0 = no limit
     *
     * @param query
     * @param limit
     * @return
     * @throws IOException
     */
    public static HashSet<String> queryDatasource(String query, int limit) throws IOException {
        return queryDatasource(query, limit, false);
    }

    /**
     * @param query
     * @param limit
     * @param inferencing
     * @return
     * @throws IOException
     */
    public static HashSet<String> queryDatasource(String query, int limit, boolean inferencing) throws IOException {
        String serverURL = _MultiMapper;
        serverURL += "rangeEnd=" + limit + "&ordered=false" +
                "&repository=jena_virtuoso_on_kdsrv" +
                "&format=csv" + (inferencing? "&inferencing=" + inferencing : "") +
                "&query=" + URLEncoder.encode(query, "UTF-8");

        System.out.println(serverURL);
        //String content = getContent("JSON", url);
        String content = getContent("application/json", new URL(serverURL));
        if (content == null)
            return null;

        HashSet<String> datasourceURIs = new HashSet<>();
        String[] split = content.split("\"");
        for (int i = 0; i < split.length; i++)
            if (!split[i].trim().isEmpty())
                datasourceURIs.add(split[i]);

        return datasourceURIs;
    }


    public static HashSet<String> querySuperQueries(String query) throws IOException {
        return queryInferencing(query, "query", "superQueries");
    }

    public static HashSet<String> querySuperProperties(String property) throws IOException {
        return queryInferencing(property, "property", "superProperties");
    }

    public static HashSet<String> querySuperTypes(String type) throws IOException {
        return queryInferencing(type, "type", "superTypes");
    }

    private static HashSet<String> queryInferencing(String parameter, String parameterName,
                                                    String returnVarName) throws IOException {

        String serverURL = _QueryInferencing;
        serverURL += parameterName + "=" + URLEncoder.encode(parameter, "UTF-8");
        System.out.println(serverURL);
        String content = getContent("plain", new URL(serverURL));

        JSONObject jsonObject = new JSONObject(content);
        HashSet<String> returnValues = new HashSet<>();
        jsonObject.getJSONArray(returnVarName).forEach(V -> returnValues.add((String) V));
        return returnValues;
    }

    public static Mapping mappingInferencing(Mapping mapping, String m) throws IOException {

        Set<String> mandatoryTypes;
        Set<String> mandatoryQueries;

        System.out.println("Starting inferencing...");

        attributes = new ArrayList<>();

        //Type Inferencing

        Set<String> types = mapping.getTypes();
        System.out.println("Types" + types);
        mandatoryTypes = new HashSet<>();

        types.forEach(x -> mandatoryTypes.add(x));
        mandatoryTypes.forEach(x -> System.out.println(x));


        type = new HashSet<>();
        mandatoryTypes.forEach(x -> {
            try {
                JSONIteratorTypes(x, (HashSet<String>) type);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        mapping.setTypes(type);

        //Query Inferencing

        Set<String> queries = mapping.getQueries();
        mandatoryQueries = new HashSet<>();
        queries.forEach(x -> mandatoryQueries.add(x));
        query = new HashSet<>();
        mandatoryQueries.forEach(x -> {
            try {
                JSONIteratorQuery(x, (HashSet<String>) query);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        mapping.setQueries(query);

        JSONObject mappings = mapping.getMappings();

        //Predicates Inferencing

        //TITLE:
        List<String> title;
        title = convertJSONArray2StringList(mapping.getMappings().getJSONArray("title"));

        attributes = new ArrayList<>();
        title.forEach(x -> {
            try {
                JSONIterator(x, attributes);
            } catch (IOException e) {
                e.printStackTrace();
            }

        });

        map.put("title", attributes);

        //ABSTRACT :
        List<String> abstracts;
        abstracts = convertJSONArray2StringList(mapping.getMappings().getJSONArray("abstract"));
        attributes = new ArrayList<>();
        abstracts.forEach(x -> {
            try {
                JSONIterator(x, attributes);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        map.put("abstract", attributes);

        //metadata_persons:
        List<String> metadata_persons;
        metadata_persons = convertJSONArray2StringList(mapping.getMappings().getJSONArray("metadata_persons"));
        attributes = new ArrayList<>();

        metadata_persons.forEach(x -> {
            try {
                JSONIterator(x, attributes);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        map.put("metadata_persons", attributes);

        //startdate:
        List<String> startDate;
        startDate = convertJSONArray2StringList(mapping.getMappings().getJSONArray("startDate"));
        attributes = new ArrayList<>();

        startDate.forEach(x -> {
            try {
                JSONIterator(x, attributes);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        map.put("startDate", attributes);

        //Metadata_Venue:
        List<String> metadataVenue;
        metadataVenue = convertJSONArray2StringList(mapping.getMappings().getJSONArray("metadata_venue"));
        attributes = new ArrayList<>();

        metadataVenue.forEach(x -> {
            try {
                JSONIterator(x.toString(), attributes);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        map.put("metadata_venue", attributes);

        //language:
        List<String> language;
        language = convertJSONArray2StringList(mapping.getMappings().getJSONArray("language"));
        attributes = new ArrayList<>();

        language.forEach(x -> {
            try {
                JSONIterator(x, attributes);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        map.put("language", attributes);

        //Concepts:
        List<String> concepts;
        concepts = convertJSONArray2StringList(mapping.getMappings().getJSONArray("concepts"));
        attributes = new ArrayList<>();

        concepts.forEach(x -> {
            try {
                JSONIterator(x.toString(), attributes);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        map.put("concepts", attributes);


        //Hier mapping für die metadata_venue_mapping
        List<String> metadata_venue_mapping = new ArrayList<>();

        JSONObject object = (JSONObject) mappings.get("metadata_venue_mapping");

        //rawName:
        List<String> rawName;
        rawName = convertJSONArray2StringList(object.getJSONArray("rawName"));
        attributes = new ArrayList<>();

//********************************************************************************************************************\\
        rawName.forEach(x -> {
            try {
                JSONIterator(x.toString(), attributes);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        map_mapping.put("rawName", attributes);

        //rawName:
        List<String> volume;
        volume = convertJSONArray2StringList(object.getJSONArray("volume"));
        attributes = new ArrayList<>();

        volume.forEach(x -> {
            try {
                JSONIterator(x.toString(), attributes);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        map_mapping.put("volume", attributes);


        //location:
        List<String> location;
        location = convertJSONArray2StringList(object.getJSONArray("location"));
        attributes = new ArrayList<>();

        location.forEach(x -> {
            try {
                JSONIterator(x.toString(), attributes);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        map_mapping.put("location", attributes);
        attributes = new ArrayList<>();

        map.put("metadata_venue_mapping", map_mapping);

//********************************************************************************************************************\\


        JSONObject objectConcept = (JSONObject) mappings.get("metadata_concept_mapping");

        //rawName:
        List<String> rawNameConcept;
        rawNameConcept = convertJSONArray2StringList(objectConcept.getJSONArray("rawName"));
        attributes = new ArrayList<>();


        rawNameConcept.forEach(x -> {
            try {
                JSONIterator(x.toString(), attributes);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        map_mapping_concept.put("rawName", attributes);


        map.put("metadata_concept_mapping", map_mapping_concept);
//********************************************************************************************************************\\

        JSONObject objectPerson = (JSONObject) mappings.get("metadata_person_mapping");


        //rawname Person:
        List<String> rawNamePerson;
        rawNamePerson = convertJSONArray2StringList(objectPerson.getJSONArray("rawName"));

        attributes = new ArrayList<>();

        rawNamePerson.forEach(x -> {
            try {
                JSONIterator(x.toString(), attributes);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        map_mapping_person.put("rawName", attributes);
        map.put("metadata_person_mapping", map_mapping_person);

//********************************************************************************************************************\\
        inferencedMapping = new JSONObject();

        inferencedMapping.put("queries", mapping.getQueries());

        inferencedMapping.put("types", mapping.getTypes());

        inferencedMapping.put("properties", mapping.getProperties());

        inferencedMapping.put("mappings", map);


        System.out.println(inferencedMapping.toString());
        try (FileWriter file = new FileWriter(m.replace(".json", "_inferenced.json"))) {
            file.write(inferencedMapping.toString());
            System.out.println("Finished... Successfully Copied JSON Object to File...");
        }

        //


        Mapping mapping1 = new Mapping(inferencedMapping.toString());


        return mapping1;
    }


    private static void JSONIterator(String p, List<String> attributes) throws IOException {
        attributes.addAll(querySuperProperties(p));
    }

    private static void JSONIteratorTypes(String t, HashSet<String> types) throws IOException {
        types.addAll(querySuperTypes(t));
    }

    private static void JSONIteratorQuery(String q, HashSet<String> queries) throws IOException {
        queries.addAll(querySuperQueries(q));
    }
}
