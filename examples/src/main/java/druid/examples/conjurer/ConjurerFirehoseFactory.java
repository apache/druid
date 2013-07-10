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
import com.metamx.druid.guava.Runnables;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.input.MapBasedInputRow;
import com.metamx.druid.realtime.firehose.Firehose;
import com.metamx.druid.realtime.firehose.FirehoseFactory;
import io.d8a.conjure.ConjurerBuilder;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@JsonTypeName("conjurer")
public class ConjurerFirehoseFactory implements FirehoseFactory
{
  private final ConjurerWrapper wrapper;
  private final long waitTime = 15L;
  private final TimeUnit unit = TimeUnit.SECONDS;

  @JsonCreator
  public ConjurerFirehoseFactory(
      @JsonProperty("startTime") Long startTime,
      @JsonProperty("stopTime") Long stopTime,
      @JsonProperty("linesPerSec") Integer linesPerSec,
      @JsonProperty("maxLines") Long maxLines,
      @JsonProperty("filePath") String filePath
  )
  {
    this(new ConjurerWrapper(new ConjurerBuilder(startTime, stopTime, null, linesPerSec, maxLines, filePath, true)));

  }

  public ConjurerFirehoseFactory(ConjurerWrapper wrapper)
  {
    this.wrapper = wrapper;
  }

  @Override
  public Firehose connect() throws IOException
  {
    wrapper.buildConjurer();
    wrapper.start();
    return new Firehose()
    {
      Map<String, Object> map = null;

      @Override
      public boolean hasMore()
      {
        map = wrapper.takeFromQueue(waitTime, unit);
        return map != null;
      }

      @Override
      public InputRow nextRow()
      {
        try {
          DateTime date = new DateTime(map.get("time"));
          return new MapBasedInputRow(date.getMillis(), new ArrayList(map.keySet()), map);
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

      }
    };
  }
}
