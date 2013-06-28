/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package druid.examples.webStream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.metamx.common.parsers.TimestampParser;
import com.metamx.druid.guava.Runnables;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.input.MapBasedInputRow;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.realtime.firehose.Firehose;
import com.metamx.druid.realtime.firehose.FirehoseFactory;
import com.metamx.emitter.EmittingLogger;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@JsonTypeName("webstream")
public class WebFirehoseFactory implements FirehoseFactory
{
  private static final EmittingLogger log = new EmittingLogger(WebFirehoseFactory.class);
  private static final int QUEUE_SIZE = 2000;
  private final String url;
  private final List<String> dimensions;
  private final String timeDimension;
  private final List<String> renamedDimensions;


  @JsonCreator
  public WebFirehoseFactory(
      @JsonProperty("url") String url,
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("renamedDimensions") List<String> renamedDimensions,
      @JsonProperty("timeDimension") String timeDimension
  )
  {
    this.url = url;
    this.dimensions = dimensions;
    this.renamedDimensions = renamedDimensions;
    this.timeDimension = timeDimension;
  }

  @Override
  public Firehose connect() throws IOException
  {
    final BlockingQueue<Map<String, Object>> queue = new ArrayBlockingQueue<Map<String, Object>>(QUEUE_SIZE);

    Runnable updateStream = new UpdateStream(new WebJsonSupplier(dimensions, url), queue, new DefaultObjectMapper());
    final ExecutorService service = Executors.newSingleThreadExecutor();
    service.submit(updateStream);

    return new Firehose()
    {
      private final Runnable doNothingRunnable = Runnables.getNoopRunnable();

      @Override
      public boolean hasMore()
      {
        return !(service.isTerminated());
      }


      @Override
      public InputRow nextRow()
      {
        try {
          Map<String, Object> processedMap = processMap(queue.take());
          DateTime date = TimestampParser.createTimestampParser("auto")
                                         .apply(processedMap.get(timeDimension).toString());
          long seconds = (long) date.getMillis();
          //the parser doesn't check for posix. Only expects iso or millis. This checks for posix
          if (new DateTime(seconds * 1000).getYear() == new DateTime().getYear()) {
            seconds = (long) date.getMillis() * 1000;
          }
          return new MapBasedInputRow(
              seconds,
              renamedDimensions,
              processedMap
          );
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      private Map<String, Object> renameKeys(Map<String, Object> update)
      {
        Map<String, Object> renamedMap = Maps.newHashMap();
        for (int iter = 0; iter < dimensions.size(); iter++) {
          if (update.get(dimensions.get(iter)) != null) {
            Object obj = update.get(dimensions.get(iter));
            renamedMap.put(renamedDimensions.get(iter), obj);
          }
        }
        if (renamedMap.get(timeDimension) == null) {
          renamedMap.put(timeDimension, System.currentTimeMillis());
        }
        return renamedMap;
      }

      private Map<String, Object> processMap(Map<String, Object> map)
      {
        Map<String, Object> renamedMap = renameKeys(map);
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
        service.shutdown();
      }

    };
  }
}
