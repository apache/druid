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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.data.input.test.TestData;
import io.druid.data.input.test.TestEnum;
import io.druid.data.input.test.TestStruct;
import io.druid.data.input.thrift.util.ThriftUtils;
import org.joda.time.DateTime;
import org.junit.Test;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class ThriftStreamInputRowParserTest
{

  private static final Function<Integer, String> intToString = new Function<Integer, String>()
  {
    @Nullable
    @Override
    public String apply(@Nullable Integer integer)
    {
      return integer.toString();
    }
  };

  public static final DateTime DATE_TIME = new DateTime(2016, 10, 19, 10, 30);
  public static final short SHORT_DIMENSION_VALUE = 2;
  public static final int INT_DIMENSION_VALUE = 3;
  public static final long LONG_DIMENSION_VALUE = 4;
  public static final double DOUBLE_DIMENSION_VALUE = 5;
  public static final boolean BOOLEAN_DIMENSION_VALUE = true;
  public static final String STRING_DIMENSION_VALUE = "6";
  public static final byte BYTE_DIMENSION_VALUE = 7;
  public static final byte[] BINARY_DIMENSION_VALUE = new byte[]{8, 9, 10};
  public static final TestEnum ENUM_DIMENSION_VALUE = TestEnum.ONE;
  public static final List<Integer> INT_LIST_DIMENSION_VALUE = Collections.singletonList(1);
  public static final Set<Integer> INT_SET_DIMENSION_VALUE = Sets.newHashSet(1);
  public static final Map<Integer, Integer> INT_INT_MAP_DIMENSION_VALUE = ImmutableMap.of(1, 1);
  public static final int STRUCT_INT_DIMENSION_VALUE = 1;
  public static final TestStruct STRUCT_DIMENSION_VALUE = new TestStruct(STRUCT_INT_DIMENSION_VALUE);
  public static final short SHORT_METRICS_VALUE = 16;
  public static final int INT_METRICS_VALUE = 17;
  public static final long LONG_METRICS_VALUE = 18;
  public static final double DOUBLE_METRICS_VALUE = 19;
  public static final String STRING_METRICS_VALUE = "20";
  public static final byte BYTE_METRICS_VALUE = 21;

  public static final String TIMESTAMP = "timestamp";
  public static final String SHORT_DIMENSION = "shortDimension";
  public static final String INT_DIMENSION = "intDimension";
  public static final String LONG_DIMENSION = "longDimension";
  public static final String DOUBLE_DIMENSION = "doubleDimension";
  public static final String BOOLEAN_DIMENSION = "booleanDimension";
  public static final String STRING_DIMENSION = "stringDimension";
  public static final String BYTE_DIMENSION = "byteDimension";
  public static final String BINARY_DIMENSION = "binaryDimension";
  public static final String ENUM_DIMENSION = "enumDimension";
  public static final String INT_LIST_DIMENSION = "intListDimension";
  public static final String INT_SET_DIMENSION = "intSetDimension";
  public static final String INT_INT_MAP_DIMENSION = "intIntMapDimension";
  public static final String STRUCT_INT_DIMENSION = "structDimension.someInt";
  public static final String SHORT_METRICS = "shortMetrics";
  public static final String INT_METRICS = "intMetrics";
  public static final String LONG_METRICS = "longMetrics";
  public static final String DOUBLE_METRICS = "doubleMetrics";
  public static final String STRING_METRICS = "stringMetrics";
  public static final String BYTE_METRICS = "byteMetrics";

  public static final Float EPSILON = 0.00001F;

  public static final List<String> DIMENSIONS = Arrays.asList(
      SHORT_DIMENSION,
      INT_DIMENSION,
      LONG_DIMENSION,
      DOUBLE_DIMENSION,
      BOOLEAN_DIMENSION,
      STRING_DIMENSION,
      BYTE_DIMENSION,
      BINARY_DIMENSION,
      ENUM_DIMENSION,
      INT_LIST_DIMENSION,
      INT_SET_DIMENSION,
      INT_INT_MAP_DIMENSION,
      STRUCT_INT_DIMENSION
  );

  public static final TimestampSpec TIMESTAMP_SPEC =
      new TimestampSpec(TIMESTAMP, "millis", null);
  public static final DimensionsSpec DIMENSIONS_SPEC =
      new DimensionsSpec(DimensionsSpec.getDefaultSchemas(DIMENSIONS), new ArrayList<String>(), null);
  public static final TimeAndDimsParseSpec PARSE_SPEC = new TimeAndDimsParseSpec(TIMESTAMP_SPEC, DIMENSIONS_SPEC);

  public static final String tClasName = "io.druid.data.input.test.TestData";

  public static final ThriftStreamInputRowParser PARSER = new ThriftStreamInputRowParser(PARSE_SPEC, tClasName);

  public static void assertInputRow(InputRow inputRow)
  {
    assertEquals(DIMENSIONS, inputRow.getDimensions());
    assertEquals(DATE_TIME.getMillis(), inputRow.getTimestampFromEpoch());

    assertEquals(buildStringList(SHORT_DIMENSION_VALUE), inputRow.getDimension(SHORT_DIMENSION));
    assertEquals(buildStringList(INT_DIMENSION_VALUE), inputRow.getDimension(INT_DIMENSION));
    assertEquals(buildStringList(LONG_DIMENSION_VALUE), inputRow.getDimension(LONG_DIMENSION));
    assertEquals(buildStringList(DOUBLE_DIMENSION_VALUE), inputRow.getDimension(DOUBLE_DIMENSION));
    assertEquals(buildStringList(BOOLEAN_DIMENSION_VALUE), inputRow.getDimension(BOOLEAN_DIMENSION));
    assertEquals(buildStringList(STRING_DIMENSION_VALUE), inputRow.getDimension(STRING_DIMENSION));
    assertEquals(buildStringList(BYTE_DIMENSION_VALUE), inputRow.getDimension(BYTE_DIMENSION));
    assertEquals(buildStringList(Arrays.toString(BINARY_DIMENSION_VALUE)), inputRow.getDimension(BINARY_DIMENSION));
    assertEquals(buildStringList(ENUM_DIMENSION_VALUE), inputRow.getDimension(ENUM_DIMENSION));
    assertEquals(
        Lists.transform(INT_LIST_DIMENSION_VALUE, intToString),
        inputRow.getDimension(INT_LIST_DIMENSION)
    );
    assertEquals(buildStringList(INT_INT_MAP_DIMENSION_VALUE), inputRow.getDimension(INT_INT_MAP_DIMENSION));
    assertEquals(buildStringList(INT_SET_DIMENSION_VALUE), inputRow.getDimension(INT_SET_DIMENSION));
    assertEquals(buildStringList(STRUCT_INT_DIMENSION_VALUE), inputRow.getDimension(STRUCT_INT_DIMENSION));

    assertEquals((long) SHORT_METRICS_VALUE, inputRow.getLongMetric(SHORT_METRICS));
    assertEquals((long) INT_METRICS_VALUE, inputRow.getLongMetric(INT_METRICS));
    assertEquals(LONG_METRICS_VALUE, inputRow.getLongMetric(LONG_METRICS));
    assertEquals((long) DOUBLE_METRICS_VALUE, inputRow.getLongMetric(DOUBLE_METRICS));
    assertEquals(Long.valueOf(STRING_METRICS_VALUE).longValue(), inputRow.getLongMetric(STRING_METRICS));
    assertEquals((long) BYTE_METRICS_VALUE, inputRow.getLongMetric(BYTE_METRICS));

    assertEquals((float) SHORT_METRICS_VALUE, inputRow.getFloatMetric(SHORT_METRICS), EPSILON);
    assertEquals((float) INT_METRICS_VALUE, inputRow.getFloatMetric(INT_METRICS), EPSILON);
    assertEquals((float) LONG_METRICS_VALUE, inputRow.getFloatMetric(LONG_METRICS), EPSILON);
    assertEquals(DOUBLE_METRICS_VALUE, inputRow.getFloatMetric(DOUBLE_METRICS), EPSILON);
    assertEquals(Float.valueOf(STRING_METRICS_VALUE), inputRow.getFloatMetric(STRING_METRICS), EPSILON);
    assertEquals((float) BYTE_METRICS_VALUE, inputRow.getFloatMetric(BYTE_METRICS), EPSILON);
  }

  public static TestData buildTestData()
  {
    TestData testData = new TestData();
    return testData
        .setTimestamp(DATE_TIME.getMillis())
        .setShortDimension(SHORT_DIMENSION_VALUE)
        .setIntDimension(INT_DIMENSION_VALUE)
        .setLongDimension(LONG_DIMENSION_VALUE)
        .setDoubleDimension(DOUBLE_DIMENSION_VALUE)
        .setBooleanDimension(BOOLEAN_DIMENSION_VALUE)
        .setStringDimension(STRING_DIMENSION_VALUE)
        .setByteDimension(BYTE_DIMENSION_VALUE)
        .setBinaryDimension(BINARY_DIMENSION_VALUE)
        .setEnumDimension(ENUM_DIMENSION_VALUE)
        .setIntListDimension(INT_LIST_DIMENSION_VALUE)
        .setIntSetDimension(INT_SET_DIMENSION_VALUE)
        .setIntIntMapDimension(INT_INT_MAP_DIMENSION_VALUE)
        .setStructDimension(STRUCT_DIMENSION_VALUE)
        .setShortMetrics(SHORT_METRICS_VALUE)
        .setIntMetrics(INT_METRICS_VALUE)
        .setLongMetrics(LONG_METRICS_VALUE)
        .setDoubleMetrics(DOUBLE_METRICS_VALUE)
        .setStringMetrics(STRING_METRICS_VALUE)
        .setByteMetrics(BYTE_METRICS_VALUE);
  }

  private static List<String> buildStringList(Object obj)
  {
    return Collections.singletonList(String.valueOf(obj));
  }

  @Test
  public void test_parse()
  {
    TestData testData = buildTestData();
    InputRow inputRow = PARSER.parse(ByteBuffer.wrap(ThriftUtils.encodeBase64String(testData).getBytes()));
    assertInputRow(inputRow);
  }

}
