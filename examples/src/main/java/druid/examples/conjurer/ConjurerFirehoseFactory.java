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

package druid.examples.conjurer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.metamx.druid.guava.Runnables;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.input.MapBasedInputRow;
import com.metamx.druid.realtime.firehose.Firehose;
import com.metamx.druid.realtime.firehose.FirehoseFactory;
import io.d8a.conjure.Conjurer;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static io.d8a.conjure.Conjurer.Builder;

@JsonTypeName("conjurer")
public class ConjurerFirehoseFactory implements FirehoseFactory
{
  private static final long waitTime = 15L;
  private static final TimeUnit unit = TimeUnit.SECONDS;
  private final Builder builder;

  @JsonCreator
  public ConjurerFirehoseFactory(
      @JsonProperty("startTime") Long startTime,
      @JsonProperty("stopTime") Long stopTime,
      @JsonProperty("linesPerSec") Integer linesPerSec,
      @JsonProperty("maxLines") Long maxLines,
      @JsonProperty("filePath") String filePath
  )
  {
    this(
        Conjurer.getBuilder()
                .withStartTime(startTime)
                .withStopTime(stopTime)
                .withMaxLines(maxLines)
                .withFilePath(filePath)
                .withLinesPerSec(linesPerSec)
    );
  }

  public ConjurerFirehoseFactory(Builder builder)
  {
    this.builder = builder;
  }

  @Override
  public Firehose connect() throws IOException
  {
    final BlockingQueue<Map<String, Object>> queue = Queues.newArrayBlockingQueue((int) waitTime);
    final Conjurer conjurer = builder.withPrinter(Conjurer.queuePrinter(queue)).build();
    conjurer.start();
    return new Firehose()
    {
      Map<String, Object> map;

      @Override
      public boolean hasMore()
      {
        try {
          map = queue.poll(waitTime, unit);
        }
        catch (InterruptedException e) {
          e.printStackTrace();
        }
        return map != null;
      }

      @Override
      public InputRow nextRow()
      {
        try {
          DateTime date = new DateTime(map.get("time"));
          System.out.println(map);
          return new MapBasedInputRow(date.getMillis(), Lists.newArrayList(map.keySet()), map);
        }
        catch (NullPointerException e) {
          throw Throwables.propagate(e);
        }
        finally {
          map = null;
        }
      }

      @Override
      public Runnable commit()
      {
        return Runnables.getNoopRunnable();
      }

      @Override
      public void close() throws IOException
      {
        conjurer.stop();
      }
    };
  }
}
