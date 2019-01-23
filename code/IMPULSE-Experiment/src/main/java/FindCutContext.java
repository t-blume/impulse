import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;

public class FindCutContext {


    public static void main(String[] args) {

        String input1 = "C:\\Users\\svenl\\Git\\bibliographic-metadata\\code\\CDMHarvester\\seedlists\\seedlist_bibo.file";
        String input2 = "C:\\Users\\svenl\\Git\\bibliographic-metadata\\code\\CDMHarvester\\seedlists\\seedlist_bibo_inferenced.file";


        HashSet<String> inputContext1;
        HashSet<String> inputContext2;

        inputContext1 = loadContexts(input1);

        inputContext2 = loadContexts(input2);


        HashSet<String> paylevelDomains1 = new HashSet<>();
        inputContext1.forEach(C -> {
            try {
                paylevelDomains1.add(new URI(C).getHost());
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        });

        HashSet<String> paylevelDomains2 = new HashSet<>();
        inputContext2.forEach(C -> {
            try {
                paylevelDomains2.add(new URI(C).getHost());
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        });


        int counter = (int) inputContext1.stream().filter(inputContext2::contains).count();


        inputContext1.forEach(X-> {
            if (!inputContext2.contains(X)){
                System.out.println(X);

            }




        });

        int counter2 = (int) paylevelDomains1.stream().filter(paylevelDomains2::contains).count();

        System.out.println("Menge 2 beinhaltet " + counter+ " / " + inputContext1.size() + " Items!");
        System.out.println("Menge 2 beinhaltet " + counter2+ " / " + paylevelDomains1.size() + " PLDs!");




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
