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

package io.druid.data.input;

import com.google.common.collect.Lists;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests protoBuf parsing.
 * To rebuild the .desc file run this command from ./processing folder as a basedir
 * protoc --descriptor_set_out=src/test/resources/prototest.desc --include_imports src/test/resources/ProtoTest.proto
 * To rebuild the .proto file and ProtoTestEventWrapper class run this command from ./processing folder as a basedir
 * protoc --java_out=src/test/java src/test/resources/ProtoTest.proto
 */
public class ProtoBufInputRowParserTest
{

  public static final String[] DIMENSIONS = new String[]{"eventType", "id", "someOtherId", "isValid"};
  public static final String[] DIMENSIONS_WITH_SUB = new String[]{"id", "description",
          "location.country", "location.state", "location.city", "location.zipcode",
          "income.income", "income.currency", "income.occupationType",
          "interests.interestName", "interests.subInterestNames", "interests.interestCode.code"
  };

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
            "prototest.desc", null
    );

    //create binary of proto test event
    DateTime dateTime = new DateTime(2012, 7, 12, 9, 30);
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

  @Test
  public void testParseSecondMessageByName() throws Exception
  {

    //configure parser with desc file
    ProtoBufInputRowParser parser = new ProtoBufInputRowParser(
            new JSONParseSpec(
                    new TimestampSpec("timestamp", "iso", null),
                    new DimensionsSpec(Arrays.asList(DIMENSIONS_WITH_SUB), Arrays.<String>asList(), null)
            ),
            "prototest.desc", "prototest.ProtoTestEventWithMessages" // package and message names from .desc file
    );

    //create binary of proto test event
    final DateTime dateTime = new DateTime(2012, 7, 12, 9, 30);
    final String id = "test id";
    final String description = "test description";
    final String country = "BY";
    final String state = "MSK";
    final String city = "Minsk";
    final String zipcode = "220000";
    final float income = 500f;
    final ProtoTestEventWrapper.ProtoTestEventWithMessages.Currency currency = ProtoTestEventWrapper.ProtoTestEventWithMessages.Currency.USD;
    final String interestName1 = "Druid";
    final float interestRate1 = 0.8f;
    final String interestName2 = "Java";
    final float interestRate2 = 0.2f;
    final List<String> interestNames = Lists.newArrayList(interestName1, interestName2);
    final List<String> subInterests = Lists.newArrayList("sub1", "sub2");
    final List<ProtoTestEventWrapper.ProtoTestEventWithMessages.OccupationType> occupationTypes = Lists.newArrayList(ProtoTestEventWrapper.ProtoTestEventWithMessages.OccupationType.SINGLE, ProtoTestEventWrapper.ProtoTestEventWithMessages.OccupationType.MULTI);
    final List<String> occupationTypeStrings = Lists.newArrayList(occupationTypes.get(0).name(), occupationTypes.get(1).name());
    final String interestCode = "A671B353C";
    ProtoTestEventWrapper.ProtoTestEventWithMessages event = ProtoTestEventWrapper.ProtoTestEventWithMessages.newBuilder()
            .setId(id)
            .setTimestamp(dateTime.toString())
            .setDescription(description)
            .setLocation(ProtoTestEventWrapper.ProtoTestEventWithMessages.Location.newBuilder()
                    .setCountry(country)
                    .setState(state)
                    .setCity(city)
                    .setZipcode(zipcode))
            .setIncome(ProtoTestEventWrapper.ProtoTestEventWithMessages.Income.newBuilder()
                    .setCurrency(ProtoTestEventWrapper.ProtoTestEventWithMessages.Currency.USD)
                    .setIncome(income)
                    .addAllOccupationType(occupationTypes))
            .addInterests(ProtoTestEventWrapper.ProtoTestEventWithMessages.Interest.newBuilder()
                    .setInterestName(interestName1)
                    .setInterestRate(interestRate1)
                    .addAllSubInterestNames(subInterests)
                    .setInterestCode(ProtoTestEventWrapper.ProtoTestEventWithMessages.Interest.InterestCode.newBuilder().setCode(interestCode)))
            .addInterests(ProtoTestEventWrapper.ProtoTestEventWithMessages.Interest.newBuilder()
                    .setInterestName(interestName2)
                    .setInterestRate(interestRate2)
                    .addAllSubInterestNames(subInterests))
            .build();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    event.writeTo(out);

    InputRow row = parser.parse(ByteBuffer.wrap(out.toByteArray()));
    System.out.println(row);
    assertEquals(Arrays.asList(DIMENSIONS_WITH_SUB), row.getDimensions());
    assertEquals(dateTime.getMillis(), row.getTimestampFromEpoch());

    assertDimensionEquals(row, "id", id);
    assertDimensionEquals(row, "description", description);
    // test access to sub-fields by dot
    assertDimensionEquals(row, "location.country", country);
    assertDimensionEquals(row, "location.state", state);
    assertDimensionEquals(row, "location.city", city);
    assertDimensionEquals(row, "location.zipcode", zipcode);
    assertEquals(income, row.getFloatMetric("income.income"), 0.1f);
    assertDimensionEquals(row, "income.currency", currency.name());
    // test access to sub-fields of collection by dot since druid support multi-values columns
    // string
    assertDimensionEqualsMultipleValues(row, "interests.interestName", interestNames.toArray(new String[interestNames.size()]), interestNames.size());
    // unique strings from deeper level
    assertDimensionEqualsMultipleValues(row, "interests.subInterestNames", subInterests.toArray(new String[subInterests.size()]), subInterests.size());
    // enum
    assertDimensionEqualsMultipleValues(row, "income.occupationType", occupationTypeStrings.toArray(new String[occupationTypeStrings.size()]), occupationTypeStrings.size());
    // 3rd level variable
    assertDimensionEquals(row, "interests.interestCode.code", interestCode);
  }

  private void assertDimensionEquals(InputRow row, String dimension, Object expected)
  {
    List<String> values = row.getDimension(dimension);
    assertEquals("invalid values array size", 1, values.size());
    assertEquals("invalid dimension value", expected, values.get(0));
  }

  private void assertDimensionEqualsMultipleValues(InputRow row, String dimension, String[] expected, int size)
  {
    List<String> values = row.getDimension(dimension);
    assertEquals("invalid values array size:" + values, size, values.size());
    assertArrayEquals("invalid list of values", expected, values.toArray());
  }

}
