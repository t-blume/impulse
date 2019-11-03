//import org.elasticsearch.action.get.GetResponse;
//import org.elasticsearch.client.Client;
//import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.common.transport.TransportAddress;
//import org.elasticsearch.transport.client.PreBuiltTransportClient;
//
//import java.io.BufferedReader;
//import java.io.FileReader;
//import java.io.IOException;
//import java.net.InetAddress;
//import java.net.UnknownHostException;
//import java.util.*;
//
//public class CutAnalysis {
//    //counter
//    private static int newAbstract = 0;
//    private static int keywords = 0;
//    private static int conceptDocuments = 0;
//    private static int keywordorconcepts = 0;
//    private static int addedConcepts = 0;
//    private static int newConcepts = 0;
//    private static int abstractWasAlreasythere = 0;
//    private static int noAddedConcept = 0;
//
//    private static int abstracts2 = 0;
//    private static int keywords2 = 0;
//    private static int concepts2 = 0;
//    private static int keywordorconcepts2 = 0;
//
//
//    private String INDEX;
//    private String TYPE;
//    private String INPUT;
//    private static HashMap<String, String> doc2Analyse = new HashMap<String, String>();
//    private static HashSet<String[]> doc2Analyse2 = new HashSet<>();
//
//
//    private static HashSet<String> id_first = new HashSet<>();
//    private static HashSet<String> id_second = new HashSet<>();
//
//    private Map<String, Object> contextURIs = new HashMap<>();
//
//
//    public CutAnalysis(String index, String type, String input) throws IOException {
//        this.INDEX = index;
//        this.TYPE = type;
//        this.INPUT = input;
//        loadInput(input);
//    }
//
//    private void loadInput(String input) throws IOException {
//        String line;
//        BufferedReader reader = new BufferedReader(new FileReader(input));
//        while ((line = reader.readLine()) != null) {
//            String[] parts = line.split("=", 2);
//            if (parts.length >= 2) {
//                String key = parts[0];
//                String value = parts[1];
//                doc2Analyse.put(key, value);
//            } else {
////                System.out.println("ignoring line: " + line);
//            }
//        }
//        reader.close();
//        String ID_first = null;
//        String ID_second = null;
//
//
//        for (String key : doc2Analyse.keySet()) {
//            String[] docIDs = new String[2];
//            int begin = doc2Analyse.get(key).indexOf("ID_first") + 9;
//            int end = begin + 20;
//            ID_first = doc2Analyse.get(key).substring(begin, end);
//
//            docIDs[0] = ID_first;
//
//
//            // id_first.add(ID_first);
//            // System.out.println(doc2Analyse.get(key));
//
//
//            int begin2 = doc2Analyse.get(key).indexOf("ID_second") + 10;
//            int end2 = begin2 + 20;
//            ID_second = doc2Analyse.get(key).substring(begin2, end2);
//            docIDs[1] = ID_second;
//
//            doc2Analyse2.add(docIDs);
//
//
//            //id_second.add(ID_second);
//            //  System.out.println(doc2Analyse.get(key));
//            // System.out.println(key + "=>" + doc2Analyse.get(key));
//
//        }
//    }
//
//
//    public void analyse() throws UnknownHostException {
//        System.out.println("");
//        System.out.println("");
//        System.out.println("Start analyse of Index... " + INDEX);
//
//        Client client = new PreBuiltTransportClient(
//                Settings.builder().put("client.transport.sniff", true)
//                        .put("cluster.name", "elasticsearch").build())
//                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
//
//        Iterator<String> iter = id_first.iterator();
//        Iterator<String> iter2 = id_second.iterator();
//
//
////        doc2Analyse2.forEach(x -> {
//
//        for (String[] x : doc2Analyse2) {
//
//
////            }
//
////            System.out.println("OwnID: " + x[0] + " EconID: " + x[1]);
//
//
//            GetResponse response = client.prepareGet(INDEX, TYPE, x[0]).get();
//            GetResponse responseEcon = client.prepareGet("econbiz", TYPE, x[1]).get();
//
////            System.out.println(response.getSource());
////            System.out.println(responseEcon.getSource());
//
//
//            Map<String, Object> ownDocument = response.getSourceAsMap();
//            Map<String, Object> econDocument = responseEcon.getSourceAsMap();
//
//
////            System.out.println("Own Abstract: " + ownDocument.get("abstract"));
////            System.out.println("Econ Abstract: " + econDocument.get("abstract"));
//
////            try {
//
//
//            if (ownDocument.get("abstract") != null && econDocument.get("abstract") == null) {
//
//                newAbstract++;
//            } else if (ownDocument.get("abstract") != null && econDocument.get("abstract") != null) {
//
//                abstractWasAlreasythere++;
//
//            }
//
//
//            if (ownDocument.get("concepts") != null && econDocument.get("subject") == null) {
//                //todo more detailed analysis-> how many subject are new?
//
//
//                List<HashMap> subjectOwn = (List) ownDocument.get("concepts");
//
//                if (subjectOwn.size() >= 1) {
//                    conceptDocuments++;
//
//
//                    newConcepts += subjectOwn.size();
//                    //System.out.println(subjectOwn.size());
//                }
//
//
//            } else if (ownDocument.get("concepts") != null && econDocument.get("subject") != null) {
//                List<HashMap> subjectOwn = (List) ownDocument.get("concepts");
//                List<String> subjectEcon = (List) econDocument.get("subject");
//
//                conceptDocuments++;
//                //  System.out.println("Next Document: ");
//                subjectOwn.forEach(y -> {
//
//                    if (y.get("label") != null) {
//
//                        for (String subject : subjectEcon) {
//                            if (!y.get("label").toString().contains(subject)) {
//                                addedConcepts++;
//
//                            } else {
//
//                                noAddedConcept++;
//                            }
//                        }
//
//
//                    }
//
//                });
//
//            }
//
//
//            if (ownDocument.get("keywords") != null && econDocument.get("subject") != null) {
//
//                List<String> subjectOwn = (List) ownDocument.get("keywords");
//                List<String> subjectEcon = (List) econDocument.get("subject");
//
//
////                System.out.println("Next Document: ");
//                subjectOwn.forEach(y -> {
//                    conceptDocuments++;
//
//
//                    if (y != null) {
//
//                        for (String subject : subjectEcon) {
//                            if (!y.contains(subject)) {
//                                addedConcepts++;
//                                break;
//                            } else {
//
//                                noAddedConcept++;
//                            }
//                        }
//
//
//                    }
//
//                });
//
//
//            }
////
////            } catch (NullPointerException e) {
////                e.printStackTrace();
////            }
////        });
//        }
//        System.out.println("New Abstracts: " + newAbstract);
//        System.out.println("Number of matched abstracts: " + abstractWasAlreasythere);
//
//        System.out.println("Documents with added concepts: " + conceptDocuments);
//
//
//        System.out.println("Number of Concepts added to Documents: " + (newConcepts + addedConcepts));
//
//        System.out.println("Number of matched Concepts: " + noAddedConcept);
//
//
//    }
//
//
//}
