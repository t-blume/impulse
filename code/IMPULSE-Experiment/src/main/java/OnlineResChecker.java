import java.io.*;
import java.net.UnknownHostException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class OnlineResChecker {

    // Pattern for recognizing a URL, based off RFC 3986
    private static final Pattern urlPattern = Pattern.compile(
            "(?:^|[\\W])((ht|f)tp(s?):\\/\\/|www\\.)"
                    + "(([\\w\\-]+\\.){1,}?([\\w\\-.~]+\\/?)*"
                    + "[\\p{Alnum}.,%_=?&#\\-+()\\[\\]\\*$~@!:/{};'öäüèÉ]*)",
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);

    private static HashMap<String, String> offlineRes = new HashMap<>();

    private static List<Path> filePaths = new ArrayList<>();


    public static void main(String[] args) throws UnknownHostException, UnsupportedEncodingException {
        List<String> paths = new ArrayList<>();

        paths.add("E:\\Bachelorarbeit\\paper\\data\\dcterms");

        for (String s : paths) {
            Path path = Paths.get(s).toAbsolutePath();

            // Continue if given path is nonexistant
            if (!Files.exists(path)) {
                System.out.println("\"" + path + "\" file not found!");
                continue;
            }

            // In case of a given directory -> check insides but disregards
            // following folders
            if (Files.isDirectory(path)) {
                traverseDirectory(path, true, filePaths);

            } else if (Files.isRegularFile(path)) {
                filePaths.add(path);
            }
        }

        doAnalysis();
    }


    private static void traverseDirectory(Path dir, boolean recursive, List<Path> paths) {
        Queue<Path> q = new ArrayDeque<>();
        q.add(dir);
        while (!q.isEmpty()) {
            Path element = q.poll();
            try {
                DirectoryStream<Path> d = Files.newDirectoryStream(element);
                for (Path p : d) {
                    if (Files.isRegularFile(p)) {
                        paths.add(p);
                    } else if (Files.isDirectory(p)) {
                        if (recursive) {
                            q.add(p);
                        }
                    }
                }
            } catch (IOException e) {
            }
        }

    }


    private static void doAnalysis() throws UnsupportedEncodingException {
        Iterator<Path> paths = filePaths.iterator();
        List<String> lines = new ArrayList<>();


        Map<String, Integer> test = new HashMap<>();

        int online = 0;
        int offline = 0;
        int redirect = 0;


//        List<String> okCode = new ArrayList<>();
//        List<String> redirectCode = new ArrayList<>();
//        List<String> errorCode = new ArrayList<>();
//        List<String> defaultCase = new ArrayList<>();

        int counter = 0;
        while (paths.hasNext()) {

            Path p = paths.next();
            System.out.println("Processing... " + p);
            FileInputStream is = null;
            try {
                InputStream gzin = Files.newInputStream(p);

                if (p.toString().endsWith(".gz")) {
                    gzin = new GZIPInputStream(gzin);
                    InputStreamReader reader = new InputStreamReader(gzin);
                    BufferedReader in = new BufferedReader(reader);
                    String readed;
                    while ((readed = in.readLine()) != null) {
                        //System.out.println(readed);
                        lines.add(readed);
//                        Matcher matcher = urlPattern.matcher(readed);
//
//                        while (matcher.find()) {
//                            if (readed.startsWith("INFO: lookup")) {
//                                // System.out.println(readed);
//
//                                lines.add(readed);
//                            }
//                        }


                    }
                }
                // Close stream
                gzin.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }


            System.out.println("Size of lines " + lines.size());

            // String code = getResponseCode(readed);
            for (String line : lines) {
                // System.out.println(line);
                counter++;

//                Matcher matcher = urlPattern.matcher(line);
//
//                while (matcher.find()) {
//                    int matchStart = matcher.start(1);
//                    int matchEnd = matcher.end();
////                                System.out.println(x.substring(matchStart,matchEnd));
//
//
////                                System.out.println(getResponseCode(x));
//
//
//                    test.put(line.substring(matchStart, matchEnd), getResponseCode(line));
//
//                }

            }


        }


        for (
                Map.Entry<String, Integer> entry : test.entrySet())

        {
            // System.out.println(entry.getKey() + " / " + entry.getValue());
            if (entry.getValue() != null) {
                if (entry.getValue().toString().startsWith("2")) {
                    online++;
                } else if (entry.getValue().toString().startsWith("3")) {
                    redirect++;
                } else if (entry.getValue().toString().startsWith("4")) {
                    offline++;
                }
            }

        }
        System.out.println(counter);

        System.out.println("total resources: " + test.size());
        System.out.println("Online ressources: " + online);
        System.out.println("redirect ressources: " + redirect);
        System.out.println("offline ressources: " + offline);


    }

    private static Integer getResponseCode(String readed) throws NumberFormatException, UnsupportedEncodingException {

//        System.out.println(readed);


        if (!containsHanScript(readed)) {
            // System.out.println(readed);
            Matcher matcher = urlPattern.matcher(readed);
            while (matcher.find()) {
                int matchEnd = matcher.end();

                String string = readed.substring(matchEnd, readed.length());


                int start = string.indexOf("status ") + 7;

                //  System.out.println(string.substring(start));
                return Integer.parseInt(string.substring(start, start + 3));
            }


        }
        return null;
    }

    public static boolean containsHanScript(String s) {
        for (int i = 0; i < s.length(); ) {
            int codepoint = s.codePointAt(i);
            i += Character.charCount(codepoint);
            if (Character.UnicodeScript.of(codepoint) == Character.UnicodeScript.HAN) {
                return true;
            }
        }
        return false;
    }
}

