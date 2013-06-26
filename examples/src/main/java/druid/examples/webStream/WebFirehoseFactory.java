package druid.examples.webStream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.input.MapBasedInputRow;
import com.metamx.druid.realtime.firehose.Firehose;
import com.metamx.druid.realtime.firehose.FirehoseFactory;
import com.metamx.emitter.EmittingLogger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@JsonTypeName("webstream")
public class WebFirehoseFactory implements FirehoseFactory
{
  private static final EmittingLogger log = new EmittingLogger(WebFirehoseFactory.class);
  private final String url;
  private final List<String> dimensions;
  private final String timeDimension;
  private final List<String> renamedDimensions;

  @JsonCreator
  public WebFirehoseFactory(
      @JsonProperty("url") String url,
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("renamedDimensions") List<String> renamedDimensions,
      @JsonProperty("timeDimension") String s
  )
  {
    this.url = url;
    this.dimensions = dimensions;
    this.renamedDimensions = renamedDimensions;
    this.timeDimension = s;
  }

  @Override
  public Firehose connect() throws IOException
  {
    final int QUEUE_SIZE = 2000;
    final BlockingQueue<Map<String, Object>> queue = new ArrayBlockingQueue<Map<String, Object>>(QUEUE_SIZE);

    Runnable updateStream = new UpdateStream(new WebJsonSupplier(dimensions, url), queue);
//    Thread.UncaughtExceptionHandler h = new Thread.UncaughtExceptionHandler()
//    {
//      public void uncaughtException(Thread th, Throwable ex)
//      {
//        log.info("Uncaught exception: " + ex);
//      }
//    };
//    final Thread streamReader = new Thread(updateStream);
//    streamReader.setUncaughtExceptionHandler(h);
//    streamReader.start();
    final ExecutorService service = Executors.newSingleThreadExecutor();
    service.submit(updateStream);

    return new Firehose()
    {
      private final Runnable doNothingRunnable = new NoopRunnable();

      @Override
      public boolean hasMore()
      {
        return !(service.isTerminated());
      }


      @Override
      public InputRow nextRow()
      {
        if (Thread.currentThread().isInterrupted()) {
          throw new RuntimeException("Interrupted, time to stop");
        }
        Map<String, Object> update;
        try {
          update = queue.take();
        }
        catch (InterruptedException e) {
          throw new RuntimeException("InterrutpedException", e);
        }
        Map<String, Object> processedMap = processMap(update);
        return new MapBasedInputRow(
            ((Integer) processedMap.get(timeDimension)).longValue() * 1000,
            renamedDimensions,
            processedMap
        );
      }

      private Map<String, Object> renameKeys(Map<String, Object> update)
      {
        Map<String, Object> renamedMap = new HashMap<String, Object>();
        int iter = 0;
        while (iter < dimensions.size()) {
          Object obj = update.get(dimensions.get(iter));
          renamedMap.put(renamedDimensions.get(iter), obj);
          iter++;
        }
        return renamedMap;
      }

      private void processNullDimensions(Map<String, Object> map)
      {
        for (String key : renamedDimensions) {
          if (map.get(key) == null) {
            if (key.equals(timeDimension)) {
              map.put(key, new Integer((int) System.currentTimeMillis() / 1000));
            } else {
              map.put(key, null);
            }
          }
        }
      }

      private Map<String, Object> processMap(Map<String, Object> map)
      {
        Map<String, Object> renamedMap = renameKeys(map);
        processNullDimensions(renamedMap);
        return renamedMap;
      }


      @Override
      public Runnable commit()
      {
        // ephemera in, ephemera out.
        return doNothingRunnable; // reuse the same object each time
      }

      @Override
      public void close() throws IOException
      {
        log.info("CLOSING!!!");
        service.shutdown();
      }

    };
  }
}
