package main.java.utils;

import org.json.JSONArray;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class MainUtils {

    /**
     * Extracts the outputfilename from the inputFiles
     *
     * @param inputFiles
     * @return outputname
     */
    public static String getFileName(List<String> inputFiles) {
        String s = "";
        for (int i = 0; i < inputFiles.size(); i++) {
            Path path = Paths.get(inputFiles.get(i));
            s += path.getFileName().toString().replaceAll("\\.[a-z]+", "");
        }
        return s;
    }

    public static boolean contains(Object[] objects, Object o) {
        if (objects == null || objects.length <= 0)
            return false;

        for (int i = 0; i < objects.length; i++) {
            if (objects[i].equals(o))
                return true;
        }
        return false;
    }

    public static String[] convertJSONArray2StringArray(JSONArray jsonArray) {
        String[] tmps = new String[jsonArray.length()];
        for (int i = 0; i < jsonArray.length(); i++)
            tmps[i] = jsonArray.get(i).toString();

        return tmps;
    }

    public static List<String> convertJSONArray2StringList(JSONArray jsonArray) {
        List<String> tmps = new ArrayList<>();
        for (int i = 0; i < jsonArray.length(); i++)
            tmps.add(jsonArray.get(i).toString());

        return tmps;
    }

    public static File createFile(String filepath) {
        File targetFile = new File(filepath);
        File parent = targetFile.getParentFile();
        if (!parent.exists() && !parent.mkdirs()) {
            throw new IllegalStateException("Couldn't create dir: " + parent);
        }
        return targetFile;
    }

    public static String editStrings(String context) throws NullPointerException, StringIndexOutOfBoundsException {
        StringBuffer t = new StringBuffer(context);
        if (t.toString().contains("<"))
            t.deleteCharAt(t.indexOf("<"));

        if (t.toString().contains(">"))
            t.deleteCharAt(t.indexOf(">"));

        return t.toString();
    }

    public static HashSet<String> loadPLD(String filename) {
        HashSet<String> PLDs = new HashSet<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = br.readLine()) != null) {
                // process the line.
                PLDs.add(line);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return PLDs;
    }

    public static String readFile(String filename) {
        if (filename == null)
            return null;
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = br.readLine()) != null)
                sb.append(line);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public static HashSet<String> loadContexts(String filename) throws IOException {
        HashSet<String> contextURIs = new HashSet<>();
        InputStreamReader is;
        if (filename.endsWith(".gz"))
            is = new InputStreamReader(new GZIPInputStream(new FileInputStream(filename)));
         else
            is = new InputStreamReader(new FileInputStream(filename));

        try (BufferedReader br = new BufferedReader(is)) {
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

    public static List<String> loadJSON(String filename) {
        int i = 0;
        if (filename == null)
            return null;

        List<String> jsonStrings = new LinkedList<>();
        //Object mapper instance
        //ObjectMapper mapper = new ObjectMapper();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            // process the line.
            while ((line = br.readLine()) != null) {
                //Convert JSON to POJO
                jsonStrings.add(line);
                i++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return jsonStrings;
    }


    public static String normalizeURL(String url) throws URISyntaxException {
        if (url == null)
            return null;
        url = url.trim();
        if (url.endsWith("/"))
            url = url.substring(0, url.length() - 1);

        URI uri = new URI(url);
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(uri.getHost() == null ? "" : uri.getHost().replaceAll("www\\.", ""));
        stringBuilder.append(uri.getPath() == null ? "" : uri.getPath());
        stringBuilder.append(uri.getQuery() == null ? "" : uri.getQuery());
        stringBuilder.append(uri.getFragment() == null ? "" : uri.getFragment());

        if (stringBuilder.toString() == null)
            System.out.println("ERRRROR: " + url);
        return stringBuilder.toString();
    }


    /**
     * proper way to extract PLD
     *
     * @param uri
     * @return
     * @throws URISyntaxException
     */
    public static String extractPLD(String uri) throws URISyntaxException {
        if (uri == null)
            return null;
        if (uri.trim().isEmpty())
            return "";

        URI parsedUri = new URI("http://" + uri);

        String pld = parsedUri.getHost();
        if (pld == null)
            return parsedUri.toString();

        return pld;
    }

}
