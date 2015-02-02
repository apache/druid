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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.data.input.Firehose;
import io.druid.data.input.InputRow;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class WebFirehoseFactoryTest
{
  private List<String> dimensions = Lists.newArrayList();
  private WebFirehoseFactory webbie;
  private WebFirehoseFactory webbie1;

  @Before
  public void setUp() throws Exception
  {
    dimensions.add("item1");
    dimensions.add("item2");
    dimensions.add("time");
    webbie = new WebFirehoseFactory(
        new UpdateStreamFactory()
        {
          @Override
          public UpdateStream build()
          {
            return new MyUpdateStream(ImmutableMap.<String,Object>of("item1", "value1", "item2", 2, "time", "1372121562"));
          }
        },
        "posix"
    );

    webbie1 = new WebFirehoseFactory(
        new UpdateStreamFactory()
        {
          @Override
          public UpdateStream build()
          {
            return new MyUpdateStream(ImmutableMap.<String,Object>of("item1", "value1", "item2", 2, "time", "1373241600000"));
          }
        },
        "auto"
    );

  }

  @Test
  public void testDimensions() throws Exception
  {
    InputRow inputRow;
    Firehose firehose = webbie.connect(null);
    if (firehose.hasMore()) {
      inputRow = firehose.nextRow();
    } else {
      throw new RuntimeException("queue is empty");
    }
    List<String> actualAnswer = inputRow.getDimensions();
    Collections.sort(actualAnswer);
    Assert.assertEquals(actualAnswer, dimensions);
  }

  @Test
  public void testPosixTimeStamp() throws Exception
  {
    InputRow inputRow;
    Firehose firehose = webbie.connect(null);
    if (firehose.hasMore()) {
      inputRow = firehose.nextRow();
    } else {
      throw new RuntimeException("queue is empty");
    }
    long expectedTime = 1372121562L * 1000L;
    Assert.assertEquals(expectedTime, inputRow.getTimestampFromEpoch());
  }

  @Test
  public void testISOTimeStamp() throws Exception
  {
    WebFirehoseFactory webbie3 = new WebFirehoseFactory(
        new UpdateStreamFactory()
        {
          @Override
          public UpdateStream build()
          {
            return new MyUpdateStream(ImmutableMap.<String,Object>of("item1", "value1", "item2", 2, "time", "2013-07-08"));
          }
        },
        "auto"
    );
    Firehose firehose1 = webbie3.connect(null);
    if (firehose1.hasMore()) {
      long milliSeconds = firehose1.nextRow().getTimestampFromEpoch();
      DateTime date = new DateTime("2013-07-08");
      Assert.assertEquals(date.getMillis(), milliSeconds);
    } else {
      Assert.assertFalse("hasMore returned false", true);
    }
  }

  @Test
  public void testAutoIsoTimeStamp() throws Exception
  {
    WebFirehoseFactory webbie2 = new WebFirehoseFactory(
        new UpdateStreamFactory()
        {
          @Override
          public UpdateStream build()
          {
            return new MyUpdateStream(ImmutableMap.<String,Object>of("item1", "value1", "item2", 2, "time", "2013-07-08"));
          }
        },
        null
    );
    Firehose firehose2 = webbie2.connect(null);
    if (firehose2.hasMore()) {
      long milliSeconds = firehose2.nextRow().getTimestampFromEpoch();
      DateTime date = new DateTime("2013-07-08");
      Assert.assertEquals(date.getMillis(), milliSeconds);
    } else {
      Assert.assertFalse("hasMore returned false", true);
    }
  }

  @Test
  public void testAutoMilliSecondsTimeStamp() throws Exception
  {
    Firehose firehose3 = webbie1.connect(null);
    if (firehose3.hasMore()) {
      long milliSeconds = firehose3.nextRow().getTimestampFromEpoch();
      DateTime date = new DateTime("2013-07-08");
      Assert.assertEquals(date.getMillis(), milliSeconds);
    } else {
      Assert.assertFalse("hasMore returned false", true);
    }
  }

  @Test
  public void testGetDimension() throws Exception
  {
    InputRow inputRow;
    Firehose firehose = webbie1.connect(null);
    if (firehose.hasMore()) {
      inputRow = firehose.nextRow();
    } else {
      throw new RuntimeException("queue is empty");
    }

    List<String> column1 = Lists.newArrayList();
    column1.add("value1");
    Assert.assertEquals(column1, inputRow.getDimension("item1"));
  }

  @Test
  public void testGetFloatMetric() throws Exception
  {
    InputRow inputRow;
    Firehose firehose = webbie1.connect(null);
    if (firehose.hasMore()) {
      inputRow = firehose.nextRow();
    } else {
      throw new RuntimeException("queue is empty");
    }

    Assert.assertEquals((float) 2.0, inputRow.getFloatMetric("item2"), 0.0f);
  }

  private static class MyUpdateStream implements UpdateStream
  {
    private static ImmutableMap<String,Object> map;
    public MyUpdateStream(ImmutableMap<String,Object> map){
        this.map=map;
    }

    @Override
    public Map<String, Object> pollFromQueue(long waitTime, TimeUnit unit) throws InterruptedException
    {
      return map;
    }

    @Override
    public String getTimeDimension()
    {
      return "time";
    }

    @Override
    public void start()
    {
    }

    @Override
    public void stop()
    {
    }
  }
}
