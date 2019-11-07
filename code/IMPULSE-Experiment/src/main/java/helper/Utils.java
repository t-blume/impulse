package helper;

import org.apache.lucene.search.spell.LevenshteinDistance;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {
    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());
        list.sort(Map.Entry.comparingByValue());


        Map<K, V> result = new LinkedHashMap<>();
        for (int i = list.size() - 1; i >= 0; i--)
            result.put(list.get(i).getKey(), list.get(i).getValue());

//        for (Map.Entry<K, V> entry : list)

        return result;
    }


    public static String normalizeURL(String url) throws URISyntaxException {
        if (url == null)
            return null;
        url = url.trim();
        if (url.endsWith("/"))
            url = url.substring(0, url.length() - 1);

        URI uri = new URI(url);
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(uri.getHost() == null? "" : uri.getHost().replaceAll("www\\.", ""));
        stringBuilder.append(uri.getPath() == null? "" : uri.getPath());
        stringBuilder.append(uri.getQuery() == null? "" : uri.getQuery());
        stringBuilder.append(uri.getFragment() == null? "" : uri.getFragment());

        if(stringBuilder.toString() == null)
            System.out.println("ERRRROR: " + url);
        return stringBuilder.toString();

    }


    public static boolean compareTitles(String title1, String title2) {
        if (title1 == null && title2 == null)
            return true;

        //numbers need to be exactly the same in the title, years matter!
        //e.g. "The Children (Scotland) Act 1995" vs. "The Children (Scotland) Act 1999"
        List<String> numbersTitle1 = getNumbers(title1);
        List<String> numbersTitle2 = getNumbers(title2);
        if(numbersTitle1.size() != numbersTitle2.size())
            return false;
        else{
            for(int i=0; i < numbersTitle1.size(); i++)
                if(!numbersTitle1.get(i).equals(numbersTitle2.get(i)))
                    return false;
        }

        LevenshteinDistance levenshteinDistance = new LevenshteinDistance();
        float distance = levenshteinDistance.getDistance(title1.replaceAll("\\d+", ""), title2.replaceAll("\\d+", ""));
        //System.out.println(title1 + " <-> " + title2 + ":= " + distance);
        return distance > 0.95;
    }

    public static boolean compareAuthors(String a1, String a2) {
        if (a1 == null && a2 == null)
            return true;


        LevenshteinDistance levenshteinDistance = new LevenshteinDistance();
        float distance = levenshteinDistance.getDistance(a1, a2);
        //TODO: what edit distance?
        return distance > 0.95;
    }


    public static String normalizeStrings(String string) {
        if(string == null)
            return null;
        return string.toLowerCase().replaceAll("\\.|\\,|\\:|\\;|\\\"", "").trim();
    }


    private static List<String> getNumbers(String string){
        Pattern p = Pattern.compile("\\d+");
        Matcher m = p.matcher(string);
        List<String> numberStrings = new LinkedList<>();
        while(m.find())
            numberStrings.add(m.group());

        return numberStrings;

    }



    /**
     * proper way to extract PLD
     * @param uri
     * @return
     * @throws URISyntaxException
     */
    public static String extractPLD(String uri) throws URISyntaxException {
        if (uri == null)
            return null;
        if(uri.trim().isEmpty())
            return "";

        URI parsedUri = new URI("http://" + uri);

        String pld = parsedUri.getHost();
        if(pld == null)
            return parsedUri.toString();

        return pld;
    }

}
