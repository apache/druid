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

import com.beust.jcommander.internal.Lists;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.realtime.firehose.Firehose;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WebFirehoseFactoryTest
{
  private List<String> dimensions;
  private WebFirehoseFactory webbie;
  private TestCaseSupplier testCaseSupplier;

  @BeforeClass
  public void setUp() throws Exception
  {
    dimensions = new ArrayList<String>();
    dimensions.add("item1");
    dimensions.add("item2");
    dimensions.add("time");
    testCaseSupplier = new TestCaseSupplier(
        "{\"item1\":\"value1\","
        + "\"item2\":2,"
        + "\"time\":1372121562 }"
    );
  }

  @Test
  public void testDimensions() throws Exception
  {
    InputRow inputRow;
    UpdateStreamFactory updateStreamFactory = new UpdateStreamFactory(testCaseSupplier, "time");
    webbie = new WebFirehoseFactory(new RenamingKeysUpdateStreamFactory(updateStreamFactory, null), "posix");
    Firehose firehose = webbie.connect();
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
    UpdateStreamFactory updateStreamFactory = new UpdateStreamFactory(testCaseSupplier, "time");
    webbie = new WebFirehoseFactory(new RenamingKeysUpdateStreamFactory(updateStreamFactory, null), "posix");
    Firehose firehose = webbie.connect();
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
    testCaseSupplier = new TestCaseSupplier(
        "{\"item1\": \"value1\","
        + "\"item2\":2,"
        + "\"time\":\"2013-07-08\"}"
    );

    UpdateStreamFactory updateStreamFactory = new UpdateStreamFactory(testCaseSupplier, "time");
    webbie = new WebFirehoseFactory(new RenamingKeysUpdateStreamFactory(updateStreamFactory, null), "iso");
    Firehose firehose1 = webbie.connect();
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
    testCaseSupplier = new TestCaseSupplier(
        "{\"item1\": \"value1\","
        + "\"item2\":2,"
        + "\"time\":\"2013-07-08\"}"
    );

    UpdateStreamFactory updateStreamFactory = new UpdateStreamFactory(testCaseSupplier, "time");
    webbie = new WebFirehoseFactory(new RenamingKeysUpdateStreamFactory(updateStreamFactory, null), null);
    Firehose firehose2 = webbie.connect();
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
    testCaseSupplier = new TestCaseSupplier(
        "{\"item1\": \"value1\","
        + "\"item2\":2,"
        + "\"time\":1373241600000}"
    );

    UpdateStreamFactory updateStreamFactory = new UpdateStreamFactory(testCaseSupplier, "time");
    webbie = new WebFirehoseFactory(new RenamingKeysUpdateStreamFactory(updateStreamFactory, null), null);
    Firehose firehose3 = webbie.connect();
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
    UpdateStreamFactory updateStreamFactory = new UpdateStreamFactory(testCaseSupplier, "time");
    webbie = new WebFirehoseFactory(new RenamingKeysUpdateStreamFactory(updateStreamFactory, null), "posix");
    Firehose firehose = webbie.connect();
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
    UpdateStreamFactory updateStreamFactory = new UpdateStreamFactory(testCaseSupplier, "time");
    webbie = new WebFirehoseFactory(new RenamingKeysUpdateStreamFactory(updateStreamFactory, null), "posix");
    Firehose firehose = webbie.connect();
    if (firehose.hasMore()) {
      inputRow = firehose.nextRow();
    } else {
      throw new RuntimeException("queue is empty");
    }

    Assert.assertEquals((float) 2.0, inputRow.getFloatMetric("item2"));
  }
}
