import javatools.parsers.Name;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.*;
import java.net.InetAddress;
import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.existsQuery;

public class FindCut extends Thread {
    private static String INDEX = "kummuliert_pld_inf";

    //    private static String INDEX = "analyzer_bibo";
    private static String INDEX2 = "econbiz";
    private static String TYPE = "publication";
    private static HashMap<String, Object> dedup = new HashMap<String, Object>();
    private static HashMap<String, Object> cut = new HashMap<String, Object>();
    private static HashMap<String, Object> cutExact = new HashMap<String, Object>();
    private static List<String> duplicates = new ArrayList<String>();
    private static HashMap<String, Object> first = new HashMap<String, Object>();
    //    private static HashMap<String, Object> second = new HashMap<String, Object>();
    private static HashSet<Object> second = new HashSet<>();
    private static Stack<Object> docStack = new Stack<Object>();
    private static int totalHits = 0;
    private static int totalHitsExact = 0;

    private static HashSet<String> error = new HashSet<>();

    //Counter und kram
    private int abstractCounter = 0;
    private HashMap<String, String> docIDs = new HashMap<>();
    static Map<String, Integer> hitsMapGlobal = new HashMap<>();
    static Map<String, Integer> matchMap= new HashMap<>();

    private Thread t;
    private static Client client;
    private static BufferedWriter writer;
    private static BufferedWriter writer2;
    private String threadName;

    private static int itemsInCut = 0;

    public static void main(String[] args) throws IOException, InterruptedException {


        System.setErr(new PrintStream(new OutputStream() {
            @Override
            public void write(int b) {
            }
        }));


        client = new PreBuiltTransportClient(
                Settings.builder().put("client.transport.sniff", true)
                        .put("cluster.name", "elasticsearch").build())
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

         writer = new BufferedWriter(new FileWriter("Cuts\\econbiz-kummuliert_pld_inf_fuzzy.txt", true));
        writer2 = new BufferedWriter(new FileWriter("Cuts\\econbiz-kummuliert_pld_inf_exact.txt", true));

        loadIndex();
        push2Stack();


        do {
            List<Thread> ts = new ArrayList<>();
            for (int i = 0; i < 30; i++) {
                FindCut findCut = new FindCut("Ct-" + i);
                ts.add(findCut);
            }

            for (Thread t : ts) {
                t.start();
            }

            for (Thread t : ts) {
                try {
                    t.join();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }

            //if (docStack.size() % 10000 == 0)
                System.out.println("Items Left in Stack: " + docStack.size());
        } while (!docStack.empty());


        for (Map.Entry<String, Object> entry : cut.entrySet()) {
            System.out.println(entry);

            String t = entry.toString();
            writer.append(t);
            writer.newLine();
        }
        writer.append("Total Hits: " + totalHits);
        writer.newLine();
        writer.append("Total Matches: " + cut.size());

        writer.close();

        for (Map.Entry<String, Object> entry : cutExact.entrySet()) {
            String t = entry.toString();

            writer2.append(t);
            writer2.newLine();
        }
        writer2.append("Total Hits: " + totalHits);
        writer2.newLine();
        writer2.append("Total Matches: " + cutExact.size());

        writer2.close();


        write2File(INDEX+"_hitMap", hitsMapGlobal);
        write2File(INDEX+"_matchMap", matchMap);


        System.out.println("totalHits: " + totalHits);
    }


    public FindCut(String name) {
        threadName = name;
    }


    public void run() {
        Map<String, Object> sourceOwn = new HashMap<String, Object>();
        //TODO: make thread safe + 1 gloal and 1 local

        if (!docStack.empty()) {
            SearchHit hitFirst = (SearchHit) docStack.pop();
            sourceOwn = hitFirst.getSourceAsMap();
            List<Object> authorListOwn;




            authorListOwn = (List) sourceOwn.get("metadata_persons");

            String titleFirst = cleanEntry(sourceOwn.get("title").toString());
            try {
                QueryBuilder queryBuilder = QueryBuilders.matchQuery("title.keyword", titleFirst).fuzziness(Fuzziness.AUTO).boost(1.0f).prefixLength(0);
                SearchRequestBuilder requestBuilder = client.prepareSearch("econbiz").setTypes(TYPE)
                        .setQuery(queryBuilder).setSize(100).setScroll(new TimeValue(600000));

                SearchResponse response = requestBuilder.get();


                Map<String, Object> source = new HashMap<String, Object>();

                if (response.getHits().totalHits >= 1)
                    System.out.println("Hits: " + response.getHits().totalHits);
                totalHits += response.getHits().totalHits;
                do {
                    for (SearchHit hit : response.getHits().getHits()) {
                        String ID_first = hitFirst.getId();
                        String ID_second = hit.getId();
                        source = hit.getSourceAsMap();
                        List<Object> authorListEcon;
                        authorListEcon = (List) source.get("creator_personal");
                        boolean found = false;


                        hitsMapGlobal.merge(source.get("title").toString(), 1, ((OLD, NEW) -> OLD + NEW));

                        if (!(authorListEcon == null) && authorListOwn != null) {
                            for (int i = 0; i < authorListOwn.size(); i++) {
                                for (int j = 0; j < authorListEcon.size(); j++) {
                                    HashMap<String, String> authorEcon = (HashMap) authorListEcon.get(j);
                                    HashMap<String, String> authorOwn = (HashMap) authorListOwn.get(i);
                                    if (authorOwn.get("name") != null && authorEcon.get("name") != null) {
                                        if (parseAuthorString(authorOwn.get("rawName")).trim()
                                                .equals(parseAuthorString(authorEcon.get("name")).trim())) {
                                            HashMap<String, Object> entry = new HashMap<>();

                                            entry.put("title_first", titleFirst);
                                            entry.put("ID_first", ID_first);
                                            entry.put("ID_second", ID_second);

                                            matchMap.merge(source.get("title").toString(), 1, ((OLD, NEW) -> OLD + NEW));

                                            if (sourceOwn.get("title").toString().equals(source.get("title"))) {
                                                cut.put(sourceOwn.get("title").toString(), entry);
                                                cutExact.put(sourceOwn.get("title").toString(), entry);

                                                System.out.println("Exact Match found! " + source.get("title") + " In Econbiz is equal to: " + titleFirst);
                                                System.out.println("Put " + titleFirst + " in List!" + " Already " + cut.size() + " items in List!");


                                            } else {
                                                cut.put(sourceOwn.get("title").toString(), entry);
                                                System.out.println("Match found! " + source.get("title") + " In Econbiz is equal to: " + titleFirst);
                                                System.out.println("Put " + titleFirst + " in List!" + " Already " + cut.size() + " items in List!");
                                            }
                                            found = true;
                                            break;
                                        }
                                    }
                                }
                                if (found) {
                                    found = false;
                                    break;
                                }
                            }
                        }
                    }
                    response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
                }
                while (response.getHits().getHits().length != 0)
                        ; // Zero hits mark the end of the scroll and the while loop.


            } catch (TooComplexToDeterminizeException e) {
                e.printStackTrace();
                error.add(e.getMessage());
                System.out.println("Error" + e.getMessage());
            } catch (NotSerializableExceptionWrapper | RemoteTransportException e) {
                e.printStackTrace();
                error.add(e.getMessage());
            }
        }
    }

    private static String parseAuthorString(String nameString) {

        if (nameString == null)
            return null;
        String rawName = nameString;
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
            return rawName;
        if (personName.familyName() != null) {
            normName = personName.familyName() + ", " + initials.trim();
        } else
            return rawName;
        return normName;

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

    private static void push2Stack() {
        Iterator<Object> iter = second.iterator();

        while (iter.hasNext()) {
            docStack.push(iter.next());
        }
    }

    private static String cleanEntry(String key) {
        String s = key;
        if (key.contains("http://www.w3.org/2001/XMLSchema#string>]")) {

            s = s.replace("<http://www.w3.org/2001/XMLSchema#string>", "");

        }


        if (key.contains("\\n")) {

            s = s.replace("\\n", "");


        }

        if (key.contains("HTML Summary of #")) {

            s = s.replace("HTML Summary of #", "");


        }

        if (key.contains("1") || key.contains("2") || key.contains("1") || key.contains("3") || key.contains("4") || key.contains("5") || key.contains("6") || key.contains("7") || key.contains("8") || key.contains("9") || key.contains("0")) {
            for (int i = 0; i < 5; i++) {
                s = s.replaceFirst("[0-9]", "");
            }


        }
        return s;

    }


    private static void write2File(String fileName, Map<String, Integer> hitsMap) {

        // Write JSON to File
        String output = "hitmaps\\";
        try (FileWriter file = new FileWriter(output + fileName + ".csv")) {
            hitsMap.forEach((K, V) -> {
                try {
                    file.write(K + " \t " + V + "\n");

                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            System.out.println("Finished... Successfully Copied JSON Object to File...");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void loadIndex() {
        System.out.println("Start with Index loading!");
        SearchResponse scrollResponse = client.prepareSearch(INDEX).setTypes(TYPE)
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(600000))
                .setQuery(existsQuery("title"))
                .setSize(100)
                .get();


        int counter = 0;
        do {
            for (SearchHit hit : scrollResponse.getHits().getHits()) {
                counter++;
//                if (counter >= 2400000) {
                second.add(hit);
//                   }
            }
            scrollResponse = client.prepareSearchScroll(scrollResponse.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
        }
        while (scrollResponse.getHits().getHits().length != 0 && second.size() <= 2000000); // Zero hits mark the end of the scroll and the while loop.
//        while (scrollResponse.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.
        System.out.println("Finished with loading Index! With " + second.size() + " Items!");
    }


}
