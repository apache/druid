package druid.examples.webStream;

import com.google.common.base.Throwables;
import com.google.common.io.InputSupplier;
import com.metamx.emitter.EmittingLogger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.Type;
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
  private BlockingQueue<Map<String, Object>> queue;
  private ObjectMapper mapper;
  private final TypeReference<HashMap<String,Object>> typeRef;

  public UpdateStream(InputSupplier supplier, BlockingQueue<Map<String, Object>> queue)
  {
    this.supplier = supplier;
    this.queue = queue;
    this.mapper = new ObjectMapper();
    this.typeRef= new TypeReference<HashMap<String, Object>>()
    {
    };
  }

  private boolean isValid(String s){
    return !(s.isEmpty());
  }

  @Override
  public void run() throws RuntimeException
  {
    try {
      BufferedReader reader = (BufferedReader) supplier.getInput();
      String line;
      while ((line = reader.readLine()) != null) {
        if (isValid(line)) {
          try {
            HashMap<String, Object> map = mapper.readValue(line, typeRef);
            queue.offer(map, 15L, TimeUnit.SECONDS);
            log.info("Successfully added to queue");
          }
          catch (JsonParseException e) {
            log.info("Invalid JSON Stream. Please check if the url returns a proper JSON stream.");
            Throwables.propagate(e);
          }
          catch (Exception e) {
            Throwables.propagate(e);
          }
        }
      }
    }
    catch (Exception e) {
      e.printStackTrace();
      Throwables.propagate(e);
    }

  }
}
