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

import com.google.common.io.InputSupplier;
import com.metamx.druid.jackson.DefaultObjectMapper;
import junit.framework.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class UpdateStreamTest
{
  private final ArrayList<String> dimensions = new ArrayList<String>();
  private InputSupplier testCaseSupplier;
  DefaultObjectMapper mapper = new DefaultObjectMapper();
  Map<String, Object> expectedAnswer = new HashMap<String, Object>();
  String timeDimension;

  @BeforeClass
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

  @Test(expectedExceptions = UnknownHostException.class)
  public void checkInvalidUrl() throws Exception
  {

    String invalidURL = "http://invalid.url";
    WebJsonSupplier supplier = new WebJsonSupplier(invalidURL);
    supplier.getInput();
  }

  @Test
  public void basicIngestionCheck() throws Exception
  {
    UpdateStream updateStream = new UpdateStream(
        testCaseSupplier,
        null,
        timeDimension
    );
    updateStream.run();
    Map<String, Object> insertedRow = updateStream.pollFromQueue();
    Assert.assertEquals(expectedAnswer, insertedRow);
  }

  //If a timestamp is missing, we should throw away the event
  @Test
  public void missingTimeStampCheck()
  {
    testCaseSupplier = new TestCaseSupplier(
        "{\"item1\": \"value1\","
        + "\"item2\":2}"
    );

    UpdateStream updateStream = new UpdateStream(
        testCaseSupplier,
        null,
        timeDimension
    );
    updateStream.run();
    Assert.assertEquals(updateStream.getQueueSize(), 0);
  }

  //If any other value is missing, we should still add the event and process it properly
  @Test
  public void otherNullValueCheck() throws Exception
  {
    testCaseSupplier = new TestCaseSupplier(
        "{\"item1\": \"value1\","
        + "\"time\":1372121562 }"
    );
    Map<String, Object> expectedAnswer = new HashMap<String, Object>();
    expectedAnswer.put("item1", "value1");
    expectedAnswer.put("time", 1372121562);
    UpdateStream updateStream = new UpdateStream(
        testCaseSupplier,
        null,
        timeDimension
    );
    updateStream.run();
    Map<String, Object> insertedRow = updateStream.pollFromQueue();
    Assert.assertEquals(expectedAnswer, insertedRow);
  }

  @Test
  public void checkRenameKeys() throws Exception
  {
    Map<String, Object> expectedAnswer = new HashMap<String, Object>();
    Map<String, String> renamedDimensions = new HashMap<String, String>();
    renamedDimensions.put("item1", "i1");
    renamedDimensions.put("item2", "i2");
    renamedDimensions.put("time", "t");

    expectedAnswer.put("i1", "value1");
    expectedAnswer.put("i2", 2);
    expectedAnswer.put("t", 1372121562);

    UpdateStream updateStream = new UpdateStream(
        testCaseSupplier,
        renamedDimensions,
        timeDimension
    );
    updateStream.run();
    Map<String, Object> inputRow = updateStream.pollFromQueue();
    Assert.assertEquals(expectedAnswer, inputRow);
  }

}
