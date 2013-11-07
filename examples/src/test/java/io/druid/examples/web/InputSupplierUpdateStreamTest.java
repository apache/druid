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

package io.druid.examples.web;

import com.google.common.io.InputSupplier;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class InputSupplierUpdateStreamTest
{
  private final long waitTime = 1L;
  private final TimeUnit unit = TimeUnit.SECONDS;
  private final ArrayList<String> dimensions = new ArrayList<String>();
  private InputSupplier testCaseSupplier;
  Map<String, Object> expectedAnswer = new HashMap<String, Object>();
  String timeDimension;

  @Before
  public void setUp()
  {
    timeDimension = "time";
    testCaseSupplier = new TestCaseSupplier(
        "{\"item1\": \"value1\","
        + "\"item2\":2,"
        + "\"time\":1372121562 }"
    );

    dimensions.add("item1");
    dimensions.add("item2");
    dimensions.add("time");

    expectedAnswer.put("item1", "value1");
    expectedAnswer.put("item2", 2);
    expectedAnswer.put("time", 1372121562);
  }


  @Test
  public void basicIngestionCheck() throws Exception
  {
    InputSupplierUpdateStream updateStream = new InputSupplierUpdateStream(
        testCaseSupplier,
        timeDimension
    );
    updateStream.start();
    Map<String, Object> insertedRow = updateStream.pollFromQueue(waitTime, unit);
    Assert.assertEquals(expectedAnswer, insertedRow);
    updateStream.stop();
  }

  //If a timestamp is missing, we should throw away the event
  @Test
  public void missingTimeStampCheck()
  {
    testCaseSupplier = new TestCaseSupplier(
        "{\"item1\": \"value1\","
        + "\"item2\":2}"
    );

    InputSupplierUpdateStream updateStream = new InputSupplierUpdateStream(
        testCaseSupplier,
        timeDimension
    );
    updateStream.start();
    Assert.assertEquals(updateStream.getQueueSize(), 0);
    updateStream.stop();
  }

  //If any other value is missing, we should still add the event and process it properly
  @Test
  public void otherNullValueCheck() throws Exception
  {
    testCaseSupplier = new TestCaseSupplier(
        "{\"item1\": \"value1\","
        + "\"time\":1372121562 }"
    );
    InputSupplierUpdateStream updateStream = new InputSupplierUpdateStream(
        testCaseSupplier,
        timeDimension
    );
    updateStream.start();
    Map<String, Object> insertedRow = updateStream.pollFromQueue(waitTime, unit);
    Map<String, Object> expectedAnswer = new HashMap<String, Object>();
    expectedAnswer.put("item1", "value1");
    expectedAnswer.put("time", 1372121562);
    Assert.assertEquals(expectedAnswer, insertedRow);
    updateStream.stop();
  }


}
