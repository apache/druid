/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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
