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
import java.util.ArrayList;
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
  private final ArrayList<String> dimensions;
  private final String timeDimension;
  private final ArrayList<String> renamedDimensions;
  private final String timeFormat;


  @JsonCreator
  public WebFirehoseFactory(
      @JsonProperty("url") String url,
      @JsonProperty("dimensions") ArrayList<String> dimensions,
      @JsonProperty("renamedDimensions") ArrayList<String> renamedDimensions,
      @JsonProperty("timeDimension") String timeDimension,
      @JsonProperty("timeFormat") String timeFormat
  )
  {
    this.url = url;
    this.dimensions = dimensions;
    this.renamedDimensions = renamedDimensions;
    this.timeDimension = timeDimension;
    this.timeFormat = timeFormat;
  }

  @Override
  public Firehose connect() throws IOException
  {
    final BlockingQueue<Map<String, Object>> queue = new ArrayBlockingQueue<Map<String, Object>>(QUEUE_SIZE);

    Runnable updateStream = new UpdateStream(
        new WebJsonSupplier(url),
        queue,
        new DefaultObjectMapper(),
        dimensions,
        renamedDimensions,
        timeDimension
    );
    final ExecutorService service = Executors.newSingleThreadExecutor();
    service.submit(updateStream);

    return new Firehose()
    {
      private final Runnable doNothingRunnable = Runnables.getNoopRunnable();

      @Override
      public boolean hasMore()
      {
        return !(service.isTerminated()) && queue.size() > 0;
      }


      @Override
      public InputRow nextRow()
      {
        try {
          Map<String, Object> map = queue.take();
          DateTime date = TimestampParser.createTimestampParser(timeFormat)
                                         .apply(map.get(timeDimension).toString());
          long seconds = (long) date.getMillis();
          return new MapBasedInputRow(
              seconds,
              renamedDimensions,
              map
          );
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
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
