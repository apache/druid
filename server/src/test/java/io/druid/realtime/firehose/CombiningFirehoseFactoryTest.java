/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.realtime.firehose;

import com.google.common.collect.Lists;

import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.InputRowParser;
import io.druid.java.util.common.parsers.ParseException;
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
      public long getLongMetric(String metric)
      {
        return new Float(metricValue).longValue();
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

  }
}
