/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.data.input;

import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.ParseSpec;
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
            new TimestampSpec("timestamp", "iso"),
            new DimensionsSpec(Arrays.asList(DIMENSIONS), Arrays.<String>asList(), null)
        ),
        "prototest.desc",
        null, null, null, null
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
