package druid.examples.webStream;

import com.google.common.io.InputSupplier;
import com.metamx.common.Pair;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class WebJsonSupplier implements InputSupplier<BufferedReader>
{
  private BlockingQueue<Pair<Map<String,Object>,Long>> queue;
  private List<String> dimensions;
  private String urlString;

  public WebJsonSupplier(List<String> dimensions, String urlString){
    this.dimensions=dimensions;
    this.urlString=urlString;
  }
  @Override
  public BufferedReader getInput() throws IOException
  {
    URL url = new URL(urlString);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setDoInput(true);
    connection.setDoOutput(true);
    connection.setRequestMethod("GET");

    BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));
    return reader;
  }
}
