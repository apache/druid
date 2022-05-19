/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.data.input.orc;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcMap;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcTimestamp;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class OrcStructConverterTest
{
  @Test
  public void testConvertRootFieldWithNonNullBooleanReturningOriginalValue()
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createBoolean(), true, new BooleanWritable(true));
  }

  @Test
  public void testConvertRootFieldWithNullBooleanReturningNull()
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createBoolean(), null, null);
  }

  @Test
  public void testConvertRootFieldWithNonNullByteReturningOriginalValue()
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createByte(), (byte) 0x10, new ByteWritable((byte) 0x10));
  }

  @Test
  public void testConvertRootFieldWithNullByteReturningNull()
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createByte(), null, null);
  }

  @Test
  public void testConvertRootFieldWithNonNullShortReturningOriginalValue()
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createShort(), (short) 128, new ShortWritable((short) 128));
  }

  @Test
  public void testConvertRootFieldWithNullShortReturningNull()
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createShort(), null, null);
  }

  @Test
  public void testConvertRootFieldWithNonNullIntReturningOriginalValue()
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createInt(), 1024, new IntWritable(1024));
  }

  @Test
  public void testConvertRootFieldWithNullIntReturningNull()
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createInt(), null, null);
  }

  @Test
  public void testConvertRootFieldWithNonNullLongReturningOriginalValue()
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createLong(), 2048L, new LongWritable(2048L));
  }

  @Test
  public void testConvertRootFieldWithNullLongReturningNull()
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createLong(), null, null);
  }

  @Test
  public void testConvertRootFieldWithNonNullFloatReturningOriginalValue()
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createFloat(), 0.1f, new FloatWritable(0.1f));
  }

  @Test
  public void testConvertRootFieldWithNullFloatReturningNull()
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createFloat(), null, null);
  }

  @Test
  public void testConvertRootFieldWithNonNullDoubleReturningOriginalValue()
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createDouble(), 1.0d, new DoubleWritable(1.0d));
  }

  @Test
  public void testConvertRootFieldWithNullDoubleReturningNull()
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createDouble(), null, null);
  }

  @Test
  public void testConvertRootFieldWithNonNullStringReturningOriginalValue()
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createString(), "string", new Text("string"));
  }

  @Test
  public void testConvertRootFieldWithNullStringReturningNull()
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createString(), null, null);
  }

  @Test
  public void testConvertRootFieldWithNonNullDateReturningOriginalValue()
  {
    final long date = DateTimes.of("2020-01-01").getMillis();
    final DateWritable dateWritable = new DateWritable(new Date(date));
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(
        converter,
        TypeDescription.createDate(),
        DateTimes.utc(dateWritable.get().getTime()),
        dateWritable
    );
  }

  @Test
  public void testConvertRootFieldWithNullDateReturningNull()
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createDate(), null, null);
  }

  @Test
  public void testConvertRootFieldWithNonNullTimestampReturningOriginalValue()
  {
    final long timestamp = DateTimes.of("2020-01-01T12:00:00").getMillis();
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createTimestamp(), timestamp, new OrcTimestamp(timestamp));
  }

  @Test
  public void testConvertRootFieldWithNullTimestampReturningNull()
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createTimestamp(), null, null);
  }

  @Test
  public void testConvertRootFieldWithNonNullBinaryReturningOriginalValue()
  {
    final byte[] bytes = StringUtils.toUtf8("binary");
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createBinary(), bytes, new BytesWritable(bytes));
  }

  @Test
  public void testConvertRootFieldWithNonNullBinaryReturningBinaryAsString()
  {
    final String string = "binary";
    final OrcStructConverter converter = new OrcStructConverter(true);
    assertConversion(
        converter,
        TypeDescription.createBinary(),
        string,
        new BytesWritable(StringUtils.toUtf8(string))
    );
  }

  @Test
  public void testConvertRootFieldWithNullBinaryReturningNull()
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createLong(), null, null);
  }

  @Test
  public void testConvertRootFieldWithNonNullDecimalReturningOriginalValue()
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createDecimal(), 20480.0, new HiveDecimalWritable(20480L));
  }

  @Test
  public void testConvertRootFieldWithNullDecimalReturningNull()
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createDecimal(), null, null);
  }

  @Test
  public void testConvertRootFieldWithNonNullVarcharReturningOriginalValue()
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createVarchar(), "varchar", new Text("varchar"));
  }

  @Test
  public void testConvertRootFieldWithNullVarcharReturningNull()
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createVarchar(), null, null);
  }

  @Test
  public void testConvertRootFieldWithNonNullCharReturningOriginalValue()
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createChar(), "char", new Text("char"));
  }

  @Test
  public void testConvertRootFieldWithNullCharReturningNull()
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, TypeDescription.createString(), null, null);
  }

  @Test
  public void testConvertRootFieldWithListOfNonNullPrimitivesReturningValuesAsTheyAre()
  {
    final TypeDescription listType = TypeDescription.createList(TypeDescription.createInt());
    final OrcList<IntWritable> orcList = new OrcList<>(listType);
    orcList.addAll(
        IntStream.range(0, 3).mapToObj(i -> new IntWritable(i * 10)).collect(Collectors.toList())
    );
    final List<Integer> expectedResult = orcList
        .stream()
        .map(IntWritable::get)
        .collect(Collectors.toList());

    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, listType, expectedResult, orcList);
  }

  @Test
  public void testConvertRootFieldWithListOfNullsReturningListOfNulls()
  {
    final TypeDescription listType = TypeDescription.createList(TypeDescription.createInt());
    final OrcList<IntWritable> orcList = new OrcList<>(listType);
    IntStream.range(0, 3).forEach(i -> orcList.add(null));
    final List<Integer> expectedResult = new ArrayList<>();
    IntStream.range(0, 3).forEach(i -> expectedResult.add(null));

    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, listType, expectedResult, orcList);
  }

  @Test
  public void testConvertRootFieldWithNullListReturningNull()
  {
    final TypeDescription listType = TypeDescription.createList(TypeDescription.createInt());
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, listType, null, null);
  }

  @Test
  public void testConvertRootFieldWithMapOfNonNullPrimitivesReturningValuesAsTheyAre()
  {
    final TypeDescription mapType = TypeDescription.createMap(
        TypeDescription.createInt(),
        TypeDescription.createFloat()
    );
    final OrcMap<IntWritable, FloatWritable> map = new OrcMap<>(mapType);
    for (int i = 0; i < 3; i++) {
      map.put(new IntWritable(i * 10), new FloatWritable(i / 10.f));
    }
    final Map<Integer, Float> expectedResult = new HashMap<>();
    for (Entry<IntWritable, FloatWritable> entry : map.entrySet()) {
      expectedResult.put(entry.getKey().get(), entry.getValue().get());
    }

    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, mapType, expectedResult, map);
  }

  @Test
  public void testConvertRootFieldWithMapOfNullValuesReturningMapOfNulls()
  {
    final TypeDescription mapType = TypeDescription.createMap(
        TypeDescription.createInt(),
        TypeDescription.createFloat()
    );
    final OrcMap<IntWritable, FloatWritable> map = new OrcMap<>(mapType);
    IntStream.range(0, 3).forEach(i -> map.put(new IntWritable(i * 10), null));
    final Map<Integer, Float> expectedResult = new HashMap<>();
    for (Entry<IntWritable, FloatWritable> entry : map.entrySet()) {
      expectedResult.put(entry.getKey().get(), null);
    }

    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, mapType, expectedResult, map);
  }

  @Test
  public void testConvertRootFieldWithNullMapReturningNull()
  {
    final TypeDescription mapType = TypeDescription.createMap(
        TypeDescription.createInt(),
        TypeDescription.createFloat()
    );
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, mapType, null, null);
  }

  @Test
  public void testConvertRootFieldWithStructOfNonNullPrimitivesReturningValuesAsTheyAre()
  {
    final TypeDescription structType = TypeDescription.createStruct();
    structType.addField("int", TypeDescription.createInt());
    structType.addField("float", TypeDescription.createFloat());
    final OrcStruct orcStruct = new OrcStruct(structType);
    orcStruct.setFieldValue("int", new IntWritable(10));
    orcStruct.setFieldValue("float", new FloatWritable(10.f));
    final Map<String, Object> expectedResult = new HashMap<>();
    expectedResult.put("int", ((IntWritable) orcStruct.getFieldValue("int")).get());
    expectedResult.put("float", ((FloatWritable) orcStruct.getFieldValue("float")).get());

    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, structType, expectedResult, orcStruct);
  }

  @Test
  public void testConvertRootFieldWithStructOfNullsReturningStructOfNulls()
  {
    final TypeDescription structType = TypeDescription.createStruct();
    structType.addField("int", TypeDescription.createInt());
    structType.addField("float", TypeDescription.createFloat());
    final OrcStruct orcStruct = new OrcStruct(structType);
    orcStruct.setFieldValue("int", null);
    orcStruct.setFieldValue("float", null);
    final Map<String, Object> expectedResult = new HashMap<>();
    expectedResult.put("int", null);
    expectedResult.put("float", null);

    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, structType, expectedResult, orcStruct);
  }

  @Test
  public void testConvertRootFieldWithNullStructReturningNull()
  {
    final TypeDescription structType = TypeDescription.createStruct();
    structType.addField("int", TypeDescription.createInt());
    structType.addField("float", TypeDescription.createFloat());
    final OrcStructConverter converter = new OrcStructConverter(false);
    assertConversion(converter, structType, null, null);
  }

  @Test
  public void testConvertRootFieldWithUnknownFieldNameReturningNull()
  {
    final Map<String, TypeDescription> types = new HashMap<>();
    types.put("int", TypeDescription.createInt());
    final TypeDescription schema = createRootSchema(types);
    final OrcStruct orcStruct = new OrcStruct(schema);
    orcStruct.setFieldValue("int", new IntWritable(1024));

    final OrcStructConverter converter = new OrcStructConverter(false);
    assertNullValue(converter, orcStruct, "unknownField");
  }

  private static TypeDescription createRootSchema(Map<String, TypeDescription> fieldTypes)
  {
    final TypeDescription schema = TypeDescription.createStruct();
    fieldTypes.forEach(schema::addField);
    return schema;
  }

  private static TypeDescription createRootSchema(String fieldName, TypeDescription fieldType)
  {
    return createRootSchema(Collections.singletonMap(fieldName, fieldType));
  }

  private static void assertConversion(
      OrcStructConverter converter,
      TypeDescription fieldType,
      @Nullable Object expectedValueAfterConversion,
      @Nullable WritableComparable actualValueInOrc
  )
  {
    final String fieldName = "field";
    final TypeDescription schema = createRootSchema(fieldName, fieldType);
    final OrcStruct orcStruct = new OrcStruct(schema);
    orcStruct.setFieldValue(fieldName, actualValueInOrc);
    if (expectedValueAfterConversion != null) {
      assertFieldValue(expectedValueAfterConversion, converter, orcStruct, fieldName);
    } else {
      assertNullValue(converter, orcStruct, fieldName);
    }
  }

  private static void assertFieldValue(
      Object expectedValue,
      OrcStructConverter converter,
      OrcStruct orcStruct,
      String fieldName
  )
  {
    final Object field = converter.convertRootField(orcStruct, fieldName);
    Assert.assertNotNull(field);
    Assert.assertEquals(expectedValue, field);
  }

  private static void assertNullValue(OrcStructConverter converter, OrcStruct orcStruct, String fieldName)
  {
    final Object field = converter.convertRootField(orcStruct, fieldName);
    Assert.assertNull(field);
  }

}
