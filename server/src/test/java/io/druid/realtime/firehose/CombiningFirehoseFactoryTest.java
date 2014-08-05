/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
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

package io.druid.realtime.firehose;

import com.google.common.collect.Lists;
import com.metamx.common.parsers.ParseException;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.InputRowParser;
import io.druid.segment.realtime.firehose.CombiningFirehoseFactory;
import io.druid.utils.Runnables;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class CombiningFirehoseFactoryTest
{

  @Test
  public void testCombiningfirehose() throws IOException
  {
    List<InputRow> list1 = Arrays.asList(makeRow(1, 1), makeRow(2, 2));
    List<InputRow> list2 = Arrays.asList(makeRow(3, 3), makeRow(4, 4), makeRow(5, 5));
    FirehoseFactory combiningFactory = new CombiningFirehoseFactory(
        Arrays.<FirehoseFactory>asList(
            new ListFirehoseFactory(list1),
            new ListFirehoseFactory(list2)
        )
    );
    final Firehose firehose = combiningFactory.connect(null);
    for (int i = 1; i < 6; i++) {
      Assert.assertTrue(firehose.hasMore());
      final InputRow inputRow = firehose.nextRow();
      Assert.assertEquals(i, inputRow.getTimestampFromEpoch());
      Assert.assertEquals(i, inputRow.getFloatMetric("test"), 0);
    }
    Assert.assertFalse(firehose.hasMore());
  }

  private InputRow makeRow(final long timestamp, final float metricValue)
  {
    return new InputRow()
    {
      @Override
      public List<String> getDimensions()
      {
        return Arrays.asList("testDim");
      }

      @Override
      public long getTimestampFromEpoch()
      {
        return timestamp;
      }

      @Override
      public DateTime getTimestamp()
      {
        return new DateTime(timestamp);
      }

      @Override
      public List<String> getDimension(String dimension)
      {
        return Lists.newArrayList();
      }

      @Override
      public float getFloatMetric(String metric)
      {
        return metricValue;
      }

      @Override
      public Object getRaw(String dimension)
      {
        return null;
      }

      @Override
      public int compareTo(Row o)
      {
        return 0;
      }
    };
  }

  public static class ListFirehoseFactory implements FirehoseFactory<InputRowParser>
  {
    private final List<InputRow> rows;

    ListFirehoseFactory(List<InputRow> rows)
    {
      this.rows = rows;
    }

    @Override
    public Firehose connect(InputRowParser inputRowParser) throws IOException, ParseException
    {
      final Iterator<InputRow> iterator = rows.iterator();
      return new Firehose()
      {
        @Override
        public boolean hasMore()
        {
          return iterator.hasNext();
        }

        @Override
        public InputRow nextRow()
        {
          return iterator.next();
        }

        @Override
        public Runnable commit()
        {
          return Runnables.getNoopRunnable();
        }

        @Override
        public void close() throws IOException
        {
          // Do nothing
        }
      };
    }

    @Override
    public InputRowParser getParser()
    {
      return null;
    }
  }
}
