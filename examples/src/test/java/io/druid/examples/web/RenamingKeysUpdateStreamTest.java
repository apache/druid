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
