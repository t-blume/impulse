package main.java.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;


/**
 * Created by Blume Till on 07.11.2016.
 */
public class WebIO {
    private static final String USER_AGENT = "Mozilla/5.0";

    public static String getContent(String contentType, URL url) throws IOException {
        HttpURLConnection con = (HttpURLConnection) url.openConnection();

        // optional default is GET
        con.setRequestMethod("GET");
        //add request header
        con.setRequestProperty("User-Agent", USER_AGENT);
        con.setRequestProperty("Accept-Charset", "utf-8");
        con.setRequestProperty("Content-Type", contentType);

        int responseCode = con.getResponseCode();
/*        System.out.println("\nSending 'GET' request to URL : " + url);
        System.out.println("Response Code : " + responseCode);*/

        if (responseCode != 200)
            return null;

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        StringBuffer response = new StringBuffer();

        try {
            int BUFFER_SIZE = 1024;
            char[] buffer = new char[BUFFER_SIZE]; // or some other size,
            int charsRead = 0;
            while ((charsRead = in.read(buffer, 0, BUFFER_SIZE)) != -1) {
                response.append(buffer, 0, charsRead);
            }
            in.close();
        }catch (IOException e){
            System.out.println(e.getLocalizedMessage());
        }

        return response.toString();
    }

}
