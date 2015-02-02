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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RenamingKeysUpdateStreamTest
{
  private final long waitTime = 15L;
  private final TimeUnit unit = TimeUnit.SECONDS;
  private InputSupplier testCaseSupplier;
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
  }

  @Test
  public void testPolFromQueue() throws Exception
  {
    InputSupplierUpdateStream updateStream = new InputSupplierUpdateStream(testCaseSupplier, timeDimension);
    Map<String, String> renamedKeys = new HashMap<String, String>();
    renamedKeys.put("item1", "i1");
    renamedKeys.put("item2", "i2");
    renamedKeys.put("time", "t");

    RenamingKeysUpdateStream renamer = new RenamingKeysUpdateStream(updateStream, renamedKeys);
    renamer.start();
    Map<String, Object> expectedAnswer = new HashMap<String, Object>();
    expectedAnswer.put("i1", "value1");
    expectedAnswer.put("i2", 2);
    expectedAnswer.put("t", 1372121562);
    Assert.assertEquals(expectedAnswer, renamer.pollFromQueue(waitTime, unit));
  }

  @Test
  public void testGetTimeDimension() throws Exception
  {
    InputSupplierUpdateStream updateStream = new InputSupplierUpdateStream(testCaseSupplier, timeDimension);
    Map<String, String> renamedKeys = new HashMap<String, String>();
    renamedKeys.put("item1", "i1");
    renamedKeys.put("item2", "i2");
    renamedKeys.put("time", "t");

    RenamingKeysUpdateStream renamer = new RenamingKeysUpdateStream(updateStream, renamedKeys);
    Assert.assertEquals("t", renamer.getTimeDimension());
  }

  @Test
  public void testMissingTimeRename() throws Exception
  {
    InputSupplierUpdateStream updateStream = new InputSupplierUpdateStream(testCaseSupplier, timeDimension);
    Map<String, String> renamedKeys = new HashMap<String, String>();
    renamedKeys.put("item1", "i1");
    renamedKeys.put("item2", "i2");
    RenamingKeysUpdateStream renamer = new RenamingKeysUpdateStream(updateStream, renamedKeys);
    Assert.assertEquals("time", renamer.getTimeDimension());
  }

}
