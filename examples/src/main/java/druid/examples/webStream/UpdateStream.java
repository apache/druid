package druid.examples.webStream;

import com.google.common.io.InputSupplier;
import com.metamx.emitter.EmittingLogger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class UpdateStream implements Runnable
{
  private static final EmittingLogger log = new EmittingLogger(UpdateStream.class);
  private InputSupplier supplier;
  private BlockingQueue<Map<String,Object>> queue;
  public UpdateStream(InputSupplier supplier,BlockingQueue<Map<String,Object>> queue, String s){
    this.supplier=supplier;
    this.queue=queue;
  }

  @Override
  public void run() throws RuntimeException
  {
    try{
      BufferedReader reader = (BufferedReader) supplier.getInput();
      String line;
      ObjectMapper mapper = new ObjectMapper();
      TypeReference<HashMap<String,Object>> typeRef = new TypeReference<HashMap<String,Object>> () {};
      while ((line = reader.readLine())!= null){
        if(!line.equals("")){
          try{
            HashMap<String,Object> map=mapper.readValue(line, typeRef);;
            queue.offer(map, 15L, TimeUnit.SECONDS);
            log.info("Successfully added to queue");
          }
          catch (JsonParseException e){
            System.out.println("Invalid JSON Stream. Please check if the url returns a proper JSON stream.");
            throw new RuntimeException("Invalid JSON Stream");
          }
          catch (Exception e){
            System.out.println(e);
            return;
          }
        }
      }
    }
    catch (MalformedURLException e){
      throw new RuntimeException("Malformed url");
    }
    catch (ProtocolException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      throw new RuntimeException(e.getMessage());
    }
    catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      throw new RuntimeException(e.getMessage());
    }

  }
}
