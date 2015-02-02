/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.examples.web;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Throwables;
import com.metamx.common.parsers.TimestampParser;
import com.metamx.emitter.EmittingLogger;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.utils.Runnables;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@JsonTypeName("webstream")
public class WebFirehoseFactory implements FirehoseFactory<InputRowParser>
{
  private static final EmittingLogger log = new EmittingLogger(WebFirehoseFactory.class);

  private final String timeFormat;
  private final UpdateStreamFactory factory;
  private final long queueWaitTime = 15L;

  @JsonCreator
  public WebFirehoseFactory(
      @JsonProperty("url") String url,
      @JsonProperty("renamedDimensions") Map<String, String> renamedDimensions,
      @JsonProperty("timeDimension") String timeDimension,
      @JsonProperty("timeFormat") String timeFormat
  )
  {
    this(
        new RenamingKeysUpdateStreamFactory(
            new InputSupplierUpdateStreamFactory(new WebJsonSupplier(url), timeDimension),
            renamedDimensions
        ), timeFormat
    );
  }

  public WebFirehoseFactory(UpdateStreamFactory factory, String timeFormat)
  {
    this.factory = factory;
    if (timeFormat == null) {
      this.timeFormat = "auto";
    } else {
      this.timeFormat = timeFormat;
    }
  }

  @Override
  public Firehose connect(InputRowParser parser) throws IOException
  {

    final UpdateStream updateStream = factory.build();
    updateStream.start();

    return new Firehose()
    {
      Map<String, Object> map;
      private final Runnable doNothingRunnable = Runnables.getNoopRunnable();

      @Override
      public boolean hasMore()
      {
        try {
          map = updateStream.pollFromQueue(queueWaitTime, TimeUnit.SECONDS);
          return map != null;
        }
        catch (InterruptedException e) {
          throw Throwables.propagate(e);
        }
      }


      @Override
      public InputRow nextRow()
      {
        try {
          DateTime date = TimestampParser.createTimestampParser(timeFormat)
                                         .apply(map.get(updateStream.getTimeDimension()).toString());
          return new MapBasedInputRow(
              date.getMillis(),
              new ArrayList(map.keySet()),
              map
          );
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
        finally {
          map = null;
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
        updateStream.stop();
      }

    };
  }

  @Override
  public InputRowParser getParser()
  {
    return null;
  }
}
