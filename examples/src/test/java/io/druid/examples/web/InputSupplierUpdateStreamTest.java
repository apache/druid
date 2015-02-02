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
