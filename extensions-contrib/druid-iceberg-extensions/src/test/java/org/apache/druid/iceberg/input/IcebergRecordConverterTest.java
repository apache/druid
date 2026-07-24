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

package org.apache.druid.iceberg.input;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class IcebergRecordConverterTest
{
  @Test
  public void testConvertPrimitiveTypes()
  {
    final Schema schema = new Schema(
        Types.NestedField.required(1, "boolCol", Types.BooleanType.get()),
        Types.NestedField.required(2, "intCol", Types.IntegerType.get()),
        Types.NestedField.required(3, "longCol", Types.LongType.get()),
        Types.NestedField.required(4, "floatCol", Types.FloatType.get()),
        Types.NestedField.required(5, "doubleCol", Types.DoubleType.get()),
        Types.NestedField.required(6, "stringCol", Types.StringType.get())
    );

    final GenericRecord record = GenericRecord.create(schema);
    record.setField("boolCol", true);
    record.setField("intCol", 42);
    record.setField("longCol", 123456789L);
    record.setField("floatCol", 3.14f);
    record.setField("doubleCol", 2.718);
    record.setField("stringCol", "hello");

    final IcebergRecordConverter converter = new IcebergRecordConverter(schema);
    final Map<String, Object> result = converter.convert(record);

    Assert.assertEquals(true, result.get("boolCol"));
    Assert.assertEquals(42, result.get("intCol"));
    Assert.assertEquals(123456789L, result.get("longCol"));
    Assert.assertEquals(3.14f, result.get("floatCol"));
    Assert.assertEquals(2.718, result.get("doubleCol"));
    Assert.assertEquals("hello", result.get("stringCol"));
  }

  @Test
  public void testConvertDateAndTimeTypes()
  {
    final Schema schema = new Schema(
        Types.NestedField.required(1, "dateCol", Types.DateType.get()),
        Types.NestedField.required(2, "timeCol", Types.TimeType.get()),
        Types.NestedField.required(3, "tsCol", Types.TimestampType.withoutZone()),
        Types.NestedField.required(4, "tstzCol", Types.TimestampType.withZone())
    );

    final GenericRecord record = GenericRecord.create(schema);
    record.setField("dateCol", LocalDate.of(2024, 6, 15));
    record.setField("timeCol", LocalTime.of(14, 30, 0));
    record.setField("tsCol", LocalDateTime.of(2024, 6, 15, 14, 30, 0));
    record.setField("tstzCol", OffsetDateTime.of(2024, 6, 15, 14, 30, 0, 0, ZoneOffset.UTC));

    final IcebergRecordConverter converter = new IcebergRecordConverter(schema);
    final Map<String, Object> result = converter.convert(record);

    Assert.assertEquals("2024-06-15", result.get("dateCol"));
    Assert.assertEquals("14:30", result.get("timeCol"));
    // Both timestamp types should return epoch millis
    Assert.assertTrue(result.get("tsCol") instanceof Long);
    Assert.assertTrue(result.get("tstzCol") instanceof Long);
    Assert.assertEquals(
        LocalDateTime.of(2024, 6, 15, 14, 30, 0).toInstant(ZoneOffset.UTC).toEpochMilli(),
        result.get("tsCol")
    );
    Assert.assertEquals(
        OffsetDateTime.of(2024, 6, 15, 14, 30, 0, 0, ZoneOffset.UTC).toInstant().toEpochMilli(),
        result.get("tstzCol")
    );
  }

  @Test
  public void testConvertDecimalType()
  {
    final Schema schema = new Schema(
        Types.NestedField.required(1, "decimalCol", Types.DecimalType.of(10, 2))
    );

    final GenericRecord record = GenericRecord.create(schema);
    record.setField("decimalCol", new BigDecimal("123.45"));

    final IcebergRecordConverter converter = new IcebergRecordConverter(schema);
    final Map<String, Object> result = converter.convert(record);

    Assert.assertEquals(123.45, (Double) result.get("decimalCol"), 0.001);
  }

  @Test
  public void testConvertBinaryType()
  {
    final Schema schema = new Schema(
        Types.NestedField.required(1, "binaryCol", Types.BinaryType.get())
    );

    final byte[] data = new byte[]{1, 2, 3, 4};
    final GenericRecord record = GenericRecord.create(schema);
    record.setField("binaryCol", ByteBuffer.wrap(data));

    final IcebergRecordConverter converter = new IcebergRecordConverter(schema);
    final Map<String, Object> result = converter.convert(record);

    Assert.assertArrayEquals(data, (byte[]) result.get("binaryCol"));
  }

  @Test
  public void testConvertNullValues()
  {
    final Schema schema = new Schema(
        Types.NestedField.optional(1, "nullableStr", Types.StringType.get()),
        Types.NestedField.optional(2, "nullableInt", Types.IntegerType.get())
    );

    final GenericRecord record = GenericRecord.create(schema);
    record.setField("nullableStr", null);
    record.setField("nullableInt", null);

    final IcebergRecordConverter converter = new IcebergRecordConverter(schema);
    final Map<String, Object> result = converter.convert(record);

    Assert.assertNull(result.get("nullableStr"));
    Assert.assertNull(result.get("nullableInt"));
  }

  @Test
  public void testConvertNestedStruct()
  {
    final Types.StructType addressType = Types.StructType.of(
        Types.NestedField.required(3, "city", Types.StringType.get()),
        Types.NestedField.required(4, "zip", Types.IntegerType.get())
    );
    final Schema schema = new Schema(
        Types.NestedField.required(1, "name", Types.StringType.get()),
        Types.NestedField.required(2, "address", addressType)
    );

    final GenericRecord addressRecord = GenericRecord.create(addressType);
    addressRecord.setField("city", "SanFrancisco");
    addressRecord.setField("zip", 94105);

    final GenericRecord record = GenericRecord.create(schema);
    record.setField("name", "Alice");
    record.setField("address", addressRecord);

    final IcebergRecordConverter converter = new IcebergRecordConverter(schema);
    final Map<String, Object> result = converter.convert(record);

    Assert.assertEquals("Alice", result.get("name"));
    @SuppressWarnings("unchecked")
    final Map<String, Object> addressMap = (Map<String, Object>) result.get("address");
    Assert.assertNotNull(addressMap);
    Assert.assertEquals("SanFrancisco", addressMap.get("city"));
    Assert.assertEquals(94105, addressMap.get("zip"));
  }

  @Test
  public void testConvertListType()
  {
    final Schema schema = new Schema(
        Types.NestedField.required(1, "tags", Types.ListType.ofRequired(2, Types.StringType.get()))
    );

    final GenericRecord record = GenericRecord.create(schema);
    record.setField("tags", Arrays.asList("tag1", "tag2", "tag3"));

    final IcebergRecordConverter converter = new IcebergRecordConverter(schema);
    final Map<String, Object> result = converter.convert(record);

    @SuppressWarnings("unchecked")
    final List<Object> tags = (List<Object>) result.get("tags");
    Assert.assertNotNull(tags);
    Assert.assertEquals(3, tags.size());
    Assert.assertEquals("tag1", tags.get(0));
    Assert.assertEquals("tag2", tags.get(1));
    Assert.assertEquals("tag3", tags.get(2));
  }
}
