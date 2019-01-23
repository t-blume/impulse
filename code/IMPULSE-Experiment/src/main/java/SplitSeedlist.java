import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class SplitSeedlist {

    private static String INDEX;
    private static String TYPE;
    private static String parameter;
    private HashSet<String> seeds = new HashSet<>();
    private String output;

    public SplitSeedlist(String index, String type, String output) {
        this.INDEX = index;
        this.TYPE = type;
        this.output = output;

    }

    public void loadInput() throws IOException {

        seeds = loadContexts("C:\\Users\\svenl\\Git\\bibliographic-metadata\\code\\CDMHarvester\\seedlists\\seedlist_dcterms_inferenced.file");
        filterSeedlist();

        //extractPLDs();
//        System.out.println(counter);
    }

    private void filterSeedlist() throws FileNotFoundException {
        List<String> countPLDs = new ArrayList<>();
        seeds.forEach(x -> {

            if (!x.contains("zbw.eu")) {

                countPLDs.add(x);

               // System.out.println(x);
            }


        });
        PrintStream out = new PrintStream(new FileOutputStream(output + "\\" + "seedlist_dcterms_inferenced_cleaned" + ".file"));

        countPLDs.forEach(x -> {
            out.append(x + "\n");

        });


    }

    public void extractPLDs() throws IOException {

        List<String> countPLDs = new ArrayList<>();

        seeds.forEach(x -> {
            try {
                URL url = new URL(x);
                if (!countPLDs.contains(url.getHost())) {

                    countPLDs.add(url.getHost());

                    System.out.println(url.getHost());
                }


            } catch (MalformedURLException e) {
                e.printStackTrace();
            }


        });
        PrintStream out = new PrintStream(new FileOutputStream(output + "\\" + 0 + ".csv"));
        int counter = 0;
        for (int i = 0; i < countPLDs.size(); i++) {
            if (i % 10 == 0) {
                counter = i;
                out = new PrintStream(new FileOutputStream(output + "\\" + counter + ".csv"));


            }


            out.append("http://" + countPLDs.get(i) + "/" + "\n");


            //    FileUtils.writeStringToFile(new File(output + "\\" + counter+".csv"), );


        }


        System.out.println("Finished... Successfully Copied JSON Object to File...");


    }


    private String getSubDomainName(URL url) {
        return url.getHost();
    }


    public static HashSet<String> loadContexts(String filename) {
        HashSet<String> contextURIs = new HashSet<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = br.readLine()) != null) {
                // process the line.
                String[] split = line.split(";");
                if (split.length > 1)
                    //contextURIs.add(split[0], Integer.valueOf(split[1]));
                    contextURIs.add(split[0]);
                else
                    //contextURIs.add(split[0], -1);
                    contextURIs.add(split[0]);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return contextURIs;
    }


}



