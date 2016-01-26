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

package io.druid.data.input;

import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ProtoBufInputRowParserTest
{

  public static final String[] DIMENSIONS = new String[]{"eventType", "id", "someOtherId", "isValid"};

  /*
  eventType = 1;

	required uint64 id = 2;
	required string timestamp = 3;
	optional uint32 someOtherId = 4;
	optional bool isValid = 5;
	optional string description = 6;

	optional float someFloatColumn = 7;
	optional uint32 someIntColumn = 8;
	optional uint64 someLongColumn = 9;
   */

  @Test
  public void testParse() throws Exception
  {

    //configure parser with desc file
    ProtoBufInputRowParser parser = new ProtoBufInputRowParser(
        new JSONParseSpec(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(Arrays.asList(DIMENSIONS), Arrays.<String>asList(), null)
        ),
        "prototest.desc"
    );


    //create binary of proto test event
    DateTime dateTime = new DateTime(2012, 07, 12, 9, 30);
    ProtoTestEventWrapper.ProtoTestEvent event = ProtoTestEventWrapper.ProtoTestEvent.newBuilder()
                                                                      .setDescription("description")
                                                                      .setEventType(ProtoTestEventWrapper.ProtoTestEvent.EventCategory.CATEGORY_ONE)
                                                                      .setId(4711L)
                                                                      .setIsValid(true)
                                                                      .setSomeOtherId(4712)
                                                                      .setTimestamp(dateTime.toString())
                                                                      .setSomeFloatColumn(47.11F)
                                                                      .setSomeIntColumn(815)
                                                                      .setSomeLongColumn(816L)
                                                                      .build();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    event.writeTo(out);

    InputRow row = parser.parse(ByteBuffer.wrap(out.toByteArray()));
    System.out.println(row);

    assertEquals(Arrays.asList(DIMENSIONS), row.getDimensions());
    assertEquals(dateTime.getMillis(), row.getTimestampFromEpoch());

    assertDimensionEquals(row, "id", "4711");
    assertDimensionEquals(row, "isValid", "true");
    assertDimensionEquals(row, "someOtherId", "4712");
    assertDimensionEquals(row, "description", "description");
    assertDimensionEquals(row, "eventType", ProtoTestEventWrapper.ProtoTestEvent.EventCategory.CATEGORY_ONE.name());


    assertEquals(47.11F, row.getFloatMetric("someFloatColumn"), 0.0);
    assertEquals(815.0F, row.getFloatMetric("someIntColumn"), 0.0);
    assertEquals(816.0F, row.getFloatMetric("someLongColumn"), 0.0);

  }

  private void assertDimensionEquals(InputRow row, String dimension, Object expected)
  {
    List<String> values = row.getDimension(dimension);
    assertEquals(1, values.size());
    assertEquals(expected, values.get(0));
  }
}
