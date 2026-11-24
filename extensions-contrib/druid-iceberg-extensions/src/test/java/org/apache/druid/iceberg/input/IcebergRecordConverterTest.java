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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Map;

public class IcebergRecordConverterTest
{
  private static final Schema SCHEMA = new Schema(
      Types.NestedField.optional(1, "ts", Types.TimestampType.withZone()),
      Types.NestedField.optional(2, "ts_local", Types.TimestampType.withoutZone()),
      Types.NestedField.optional(3, "d", Types.DateType.get()),
      Types.NestedField.optional(4, "t", Types.TimeType.get())
  );

  @Test
  public void testJavaTimeValuesConvertedToMillis()
  {
    final Record record = GenericRecord.create(SCHEMA);
    record.setField("ts", OffsetDateTime.of(2024, 1, 2, 3, 4, 5, 0, ZoneOffset.UTC));
    record.setField("ts_local", LocalDateTime.of(2024, 1, 2, 3, 4, 5));
    record.setField("d", LocalDate.of(2024, 1, 2));
    record.setField("t", LocalTime.of(1, 2, 3));

    final Map<String, Object> row = IcebergRecordConverter.convertToMap(record, SCHEMA, false);

    final long expectedMillis = 1704164645000L;
    Assertions.assertEquals(expectedMillis, row.get("ts"));
    Assertions.assertEquals(expectedMillis, row.get("ts_local"));
    Assertions.assertEquals(1704153600000L, row.get("d"));
    Assertions.assertEquals(3_723_000L, row.get("t"));
  }

  @Test
  public void testNumericTemporalValuesConvertedToDruidUnits()
  {
    final Record record = GenericRecord.create(SCHEMA);
    record.setField("ts", 1704164645000000L);
    record.setField("ts_local", 1704164645000000L);
    record.setField("d", 19724);
    record.setField("t", 3_723_000_000L);

    final Map<String, Object> row = IcebergRecordConverter.convertToMap(record, SCHEMA, false);

    Assertions.assertEquals(1704164645000L, row.get("ts"));
    Assertions.assertEquals(1704164645000L, row.get("ts_local"));
    Assertions.assertEquals(1704153600000L, row.get("d"));
    Assertions.assertEquals(3_723_000L, row.get("t"));
  }

  @Test
  public void testNanosecondTimestampsConvertedToMillis()
  {
    final Schema schema = new Schema(
        Types.NestedField.optional(1, "ts_nano", Types.TimestampNanoType.withZone()),
        Types.NestedField.optional(2, "ts_nano_local", Types.TimestampNanoType.withoutZone()),
        Types.NestedField.optional(3, "ts_nano_raw", Types.TimestampNanoType.withoutZone())
    );
    final Record record = GenericRecord.create(schema);
    record.setField("ts_nano", OffsetDateTime.of(2024, 1, 2, 3, 4, 5, 123_456_789, ZoneOffset.UTC));
    record.setField("ts_nano_local", LocalDateTime.of(2024, 1, 2, 3, 4, 5, 123_456_789));
    record.setField("ts_nano_raw", 1704164645123456789L);

    final Map<String, Object> row = IcebergRecordConverter.convertToMap(record, schema, false);

    Assertions.assertEquals(1704164645123L, row.get("ts_nano"));
    Assertions.assertEquals(1704164645123L, row.get("ts_nano_local"));
    Assertions.assertEquals(1704164645123L, row.get("ts_nano_raw"));
  }

  @Test
  public void testTimestampSpecReadsConvertedTimestamp()
  {
    final Record record = GenericRecord.create(SCHEMA);
    record.setField("ts", 1704164645000000L);

    final Map<String, Object> row = IcebergRecordConverter.convertToMap(record, SCHEMA, false);
    final InputRowSchema inputRowSchema = new InputRowSchema(
        new TimestampSpec("ts", "millis", null),
        DimensionsSpec.EMPTY,
        null
    );

    Assertions.assertEquals(
        1704164645000L,
        IcebergRecordConverter.toInputRow(row, inputRowSchema).getTimestampFromEpoch()
    );
  }

  @Test
  public void testComplexColumnsNotDiscoveredWithoutSchemaDiscovery()
  {
    final Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.StringType.get()),
        Types.NestedField.optional(2, "tags", Types.ListType.ofOptional(3, Types.StringType.get())),
        Types.NestedField.optional(
            4,
            "payload",
            Types.StructType.of(Types.NestedField.optional(5, "x", Types.StringType.get()))
        )
    );
    final Record record = GenericRecord.create(schema);
    record.setField("id", "a");
    record.setField("tags", ImmutableList.of("t1"));

    final Map<String, Object> restricted = IcebergRecordConverter.convertToMap(record, schema, false, false);
    Assertions.assertEquals(Collections.singleton("id"), restricted.keySet());
    // Lookups still resolve hidden columns so explicit dimensions and transforms keep working.
    Assertions.assertEquals(ImmutableList.of("t1"), restricted.get("tags"));
    Assertions.assertTrue(restricted.containsKey("payload"));

    final Map<String, Object> discovered = IcebergRecordConverter.convertToMap(record, schema, false, true);
    Assertions.assertEquals(ImmutableSet.of("id", "tags", "payload"), discovered.keySet());

    final InputRowSchema inputRowSchema = new InputRowSchema(
        new TimestampSpec(null, null, DateTimes.of("2024-01-01")),
        DimensionsSpec.EMPTY,
        null
    );
    Assertions.assertEquals(
        ImmutableList.of("id"),
        IcebergRecordConverter.toInputRow(restricted, inputRowSchema).getDimensions()
    );
  }
}
