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

package io.druid.examples.conjure;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Lists;
import com.metamx.common.logger.Logger;
import io.d8a.conjure.Clock;
import io.d8a.conjure.ConjureTemplate;
import io.d8a.conjure.ConjureTemplateParser;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;


@JsonTypeName("conjure")
public class ConjureFirehoseFactory implements FirehoseFactory
{

  private static final Logger log = new Logger(ConjureFirehoseFactory.class);

  private final Long sleepMs;
  private final Long eventsPerSleep;
  private final Long maxGeneratedRows;
  private final String conjurePatternFile;

  public ConjureFirehoseFactory(
      @JsonProperty("conjurePatternFile") String conjurePatternFile,
      @JsonProperty("maxGeneratedRows") Long maxGeneratedRows,
      @JsonProperty("eventsPerSleep") Long eventsPerSleep,
      @JsonProperty("sleepMs") Long sleepMs
  )
  {
    this.sleepMs = sleepMs;
    this.maxGeneratedRows = maxGeneratedRows;
    this.eventsPerSleep = eventsPerSleep;
    this.conjurePatternFile = conjurePatternFile;
  }

  @Override
  public Firehose connect() throws IOException
  {
    log.info("Using conjure file: %s", conjurePatternFile);
    ConjureTemplateParser parser = new ConjureTemplateParser(Clock.SYSTEM_CLOCK);
    final ConjureTemplate template = parser.jsonParse(conjurePatternFile);
    log.info("Generating event at approximate rate of %.2f events/sec.", (eventsPerSleep * 1000.0 / sleepMs));

    return new Firehose()
    {
      private long rowCount = 0;

      @Override
      public boolean hasMore()
      {
        return maxGeneratedRows < 0 || rowCount < maxGeneratedRows;
      }

      @Override
      public InputRow nextRow()
      {
        if (rowCount % eventsPerSleep == 0) {
          try {
            Thread.sleep(sleepMs);
          }
          catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
        if (++rowCount % 20000 == 0) {
          log.info("Generated %d events.", rowCount);
        }

        Map<String, Object> event = template.conjureMapData();
        return new MapBasedInputRow((Long) event.get("timestamp"), Lists.newArrayList(event.keySet()), event);
      }

      @Override
      public Runnable commit()
      {
        return new Runnable()
        {
          @Override
          public void run()
          {
          }
        };

      }

      @Override
      public void close() throws IOException
      {
      }
    };
  }

  @JsonProperty
  public Long getSleepMs()
  {
    return sleepMs;
  }

  @JsonProperty
  public Long getMaxGeneratedRows()
  {
    return maxGeneratedRows;
  }

  @JsonProperty
  public String getConjurePatternFile()
  {
    return conjurePatternFile;
  }

  @JsonProperty
  public Long getEventsPerSleep()
  {
    return eventsPerSleep;
  }

}
