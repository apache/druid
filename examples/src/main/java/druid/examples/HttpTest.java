package druid.examples;

import org.apache.http.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * User: dhruvparthasarathy
 * Date: 6/20/13
 * Time: 12:13 PM
 * To change this template use File | Settings | File Templates.
 */
public class HttpTest
{
  public static void main(String[] args) throws Exception{
    URL url = new URL("http://developer.usa.gov/1usagov");
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setDoInput(true);
    connection.setDoOutput(true);
    connection.setRequestMethod("GET");

    BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));
    String line;
    ObjectMapper mapper = new ObjectMapper();
    TypeReference<HashMap<String,Object>> typeRef = new TypeReference<HashMap<String,Object>> () {};
    try{
    while ((line = reader.readLine())!= null){
      try{
        HashMap<String,Object> map=mapper.readValue(line, typeRef);
        System.out.println(map);
      }
      catch (Exception e){
        System.out.println(e);
      }
    }
    }
    catch (IOException e){

    }
  }
}
