package druid.examples.webStream;

import com.google.common.io.InputSupplier;
import com.metamx.emitter.EmittingLogger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;

public class WebJsonSupplier implements InputSupplier<BufferedReader>
{
  private List<String> dimensions;
  private static final EmittingLogger log = new EmittingLogger(WebJsonSupplier.class);

  private String urlString;
  private URL url;

  public WebJsonSupplier(List<String> dimensions, String urlString)
  {
    this.dimensions = dimensions;
    this.urlString = urlString;
    try{
      this.url = new URL(urlString);
    }
    catch (Exception e){
      e.printStackTrace();
      log.info("Malformed url");
    }
  }

  @Override
  public BufferedReader getInput() throws IOException
  {
    URL url = new URL(urlString);
    URLConnection connection =  url.openConnection();
    connection.setDoInput(true);
    //connection.setDoOutput(true);

    BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));
    return reader;
  }
}
