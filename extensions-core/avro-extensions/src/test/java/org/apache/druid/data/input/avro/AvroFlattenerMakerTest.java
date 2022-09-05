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

package org.apache.druid.data.input.avro;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.druid.data.input.AvroStreamInputRowParserTest;
import org.apache.druid.data.input.SomeAvroDatum;
import org.apache.druid.data.input.UnionSubEnum;
import org.apache.druid.data.input.UnionSubFixed;
import org.apache.druid.data.input.UnionSubRecord;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroFlattenerMakerTest
{
  private static final AvroFlattenerMaker FLATTENER_WITHOUT_EXTRACT_UNION_BY_TYPE =
      new AvroFlattenerMaker(false, false, false);
  private static final AvroFlattenerMaker FLATTENER_WITH_EXTRACT_UNION_BY_TYPE =
      new AvroFlattenerMaker(false, false, true);

  private static final SomeAvroDatum RECORD = AvroStreamInputRowParserTest.buildSomeAvroDatum();

  @Test
  public void getRootField_flattenerWithoutExtractUnionsByType()
  {
    getRootField_common(RECORD, FLATTENER_WITHOUT_EXTRACT_UNION_BY_TYPE);
  }

  @Test
  public void getRootField_flattenerWithExtractUnionsByType()
  {
    getRootField_common(RECORD, FLATTENER_WITH_EXTRACT_UNION_BY_TYPE);
  }

  @Test
  public void makeJsonPathExtractor_flattenerWithoutExtractUnionsByType()
  {
    makeJsonPathExtractor_common(RECORD, FLATTENER_WITHOUT_EXTRACT_UNION_BY_TYPE);
  }

  @Test
  public void makeJsonPathExtractor_flattenerWithExtractUnionsByType()
  {
    makeJsonPathExtractor_common(RECORD, FLATTENER_WITH_EXTRACT_UNION_BY_TYPE);
    Assert.assertEquals(
        RECORD.getSomeMultiMemberUnion(),
        FLATTENER_WITH_EXTRACT_UNION_BY_TYPE.makeJsonPathExtractor("$.someMultiMemberUnion.int").apply(RECORD)
    );
  }

  @Test
  public void jsonPathExtractorExtractUnionsByType()
  {
    final AvroFlattenerMaker flattener = new AvroFlattenerMaker(false, false, true);

    // Unmamed types are accessed by type

    // int
    Assert.assertEquals(1, flattener.makeJsonPathExtractor("$.someMultiMemberUnion.int").apply(
        AvroStreamInputRowParserTest.buildSomeAvroDatumWithUnionValue(1)));

    // long
    Assert.assertEquals(1L, flattener.makeJsonPathExtractor("$.someMultiMemberUnion.long").apply(
        AvroStreamInputRowParserTest.buildSomeAvroDatumWithUnionValue(1L)));

    // float
    Assert.assertEquals((float) 1.0, flattener.makeJsonPathExtractor("$.someMultiMemberUnion.float").apply(
        AvroStreamInputRowParserTest.buildSomeAvroDatumWithUnionValue((float) 1.0)));

    // double
    Assert.assertEquals(1.0, flattener.makeJsonPathExtractor("$.someMultiMemberUnion.double").apply(
        AvroStreamInputRowParserTest.buildSomeAvroDatumWithUnionValue(1.0)));

    // string
    Assert.assertEquals("string", flattener.makeJsonPathExtractor("$.someMultiMemberUnion.string").apply(
        AvroStreamInputRowParserTest.buildSomeAvroDatumWithUnionValue(new Utf8("string"))));

    // bytes
    Assert.assertArrayEquals(new byte[] {1}, (byte[]) flattener.makeJsonPathExtractor("$.someMultiMemberUnion.bytes").apply(
        AvroStreamInputRowParserTest.buildSomeAvroDatumWithUnionValue(ByteBuffer.wrap(new byte[] {1}))));

    // map
    Assert.assertEquals(2, flattener.makeJsonPathExtractor("$.someMultiMemberUnion.map.two").apply(
        AvroStreamInputRowParserTest.buildSomeAvroDatumWithUnionValue(new HashMap<String, Integer>() {{
            put("one", 1);
            put("two", 2);
            put("three", 3);
          }
        }
        )));

    // array
    Assert.assertEquals(3, flattener.makeJsonPathExtractor("$.someMultiMemberUnion.array[2]").apply(
        AvroStreamInputRowParserTest.buildSomeAvroDatumWithUnionValue(Arrays.asList(1, 2, 3))));

    // Named types are accessed by name

    // record
    Assert.assertEquals("subRecordString", flattener.makeJsonPathExtractor("$.someMultiMemberUnion.UnionSubRecord.subString").apply(
        AvroStreamInputRowParserTest.buildSomeAvroDatumWithUnionValue(
            UnionSubRecord.newBuilder()
                          .setSubString("subRecordString")
                          .build())));

    // fixed
    final byte[] fixedBytes = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    Assert.assertEquals(fixedBytes, flattener.makeJsonPathExtractor("$.someMultiMemberUnion.UnionSubFixed").apply(
        AvroStreamInputRowParserTest.buildSomeAvroDatumWithUnionValue(new UnionSubFixed(fixedBytes))));

    // enum
    Assert.assertEquals(String.valueOf(UnionSubEnum.ENUM1), flattener.makeJsonPathExtractor("$.someMultiMemberUnion.UnionSubEnum").apply(
        AvroStreamInputRowParserTest.buildSomeAvroDatumWithUnionValue(UnionSubEnum.ENUM1)));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void makeJsonQueryExtractor_flattenerWithoutExtractUnionsByType()
  {
    Assert.assertEquals(
        RECORD.getTimestamp(),
        FLATTENER_WITHOUT_EXTRACT_UNION_BY_TYPE.makeJsonQueryExtractor("$.timestamp").apply(RECORD)
    );
  }

  @Test(expected = UnsupportedOperationException.class)
  public void makeJsonQueryExtractor_flattenerWithExtractUnionsByType()
  {
    Assert.assertEquals(
        RECORD.getTimestamp(),
        FLATTENER_WITH_EXTRACT_UNION_BY_TYPE.makeJsonQueryExtractor("$.timestamp").apply(RECORD)
    );
  }

  private void getRootField_common(final SomeAvroDatum record, final AvroFlattenerMaker flattener)
  {
    Assert.assertEquals(
        record.getTimestamp(),
        flattener.getRootField(record, "timestamp")
    );
    Assert.assertEquals(
        record.getEventType(),
        flattener.getRootField(record, "eventType")
    );
    Assert.assertEquals(
        record.getId(),
        flattener.getRootField(record, "id")
    );
    Assert.assertEquals(
        record.getSomeOtherId(),
        flattener.getRootField(record, "someOtherId")
    );
    Assert.assertEquals(
        record.getIsValid(),
        flattener.getRootField(record, "isValid")
    );
    Assert.assertEquals(
        record.getSomeIntArray(),
        flattener.getRootField(record, "someIntArray")
    );
    Assert.assertEquals(
        record.getSomeStringArray(),
        flattener.getRootField(record, "someStringArray")
    );
    Assert.assertEquals(
        record.getSomeIntValueMap(),
        flattener.getRootField(record, "someIntValueMap")
    );
    Assert.assertEquals(
        record.getSomeStringValueMap(),
        flattener.getRootField(record, "someStringValueMap")
    );
    Assert.assertEquals(
        record.getSomeUnion(),
        flattener.getRootField(record, "someUnion")
    );
    Assert.assertEquals(
        record.getSomeNull(),
        flattener.getRootField(record, "someNull")
    );
    Assert.assertEquals(
        // Casted to an array by transformValue
        record.getSomeFixed().bytes(),
        flattener.getRootField(record, "someFixed")
    );
    Assert.assertEquals(
        // Casted to an array by transformValue
        record.getSomeBytes().array(),
        flattener.getRootField(record, "someBytes")
    );
    Assert.assertEquals(
        // Casted to a string by transformValue
        record.getSomeEnum().toString(),
        flattener.getRootField(record, "someEnum")
    );
    Map<String, Object> map = new HashMap<>();
    record.getSomeRecord()
          .getSchema()
          .getFields()
          .forEach(field -> map.put(field.name(), record.getSomeRecord().get(field.name())));
    Assert.assertEquals(
        map,
        flattener.getRootField(record, "someRecord")
    );
    Assert.assertEquals(
        record.getSomeLong(),
        flattener.getRootField(record, "someLong")
    );
    Assert.assertEquals(
        record.getSomeInt(),
        flattener.getRootField(record, "someInt")
    );
    Assert.assertEquals(
        record.getSomeFloat(),
        flattener.getRootField(record, "someFloat")
    );
    List<Map<String, Object>> list = new ArrayList<>();
    for (GenericRecord genericRecord : record.getSomeRecordArray()) {
      Map<String, Object> map1 = new HashMap<>();
      genericRecord
          .getSchema()
          .getFields()
          .forEach(field -> map1.put(field.name(), genericRecord.get(field.name())));
      list.add(map1);
    }
    Assert.assertEquals(
        list,
        flattener.getRootField(record, "someRecordArray")
    );
  }

  private void makeJsonPathExtractor_common(final SomeAvroDatum record, final AvroFlattenerMaker flattener)
  {
    Assert.assertEquals(
        record.getTimestamp(),
        flattener.makeJsonPathExtractor("$.timestamp").apply(record)
    );
    Assert.assertEquals(
        record.getEventType(),
        flattener.makeJsonPathExtractor("$.eventType").apply(record)
    );
    Assert.assertEquals(
        record.getId(),
        flattener.makeJsonPathExtractor("$.id").apply(record)
    );
    Assert.assertEquals(
        record.getSomeOtherId(),
        flattener.makeJsonPathExtractor("$.someOtherId").apply(record)
    );
    Assert.assertEquals(
        record.getIsValid(),
        flattener.makeJsonPathExtractor("$.isValid").apply(record)
    );
    Assert.assertEquals(
        record.getSomeIntArray(),
        flattener.makeJsonPathExtractor("$.someIntArray").apply(record)
    );
    Assert.assertEquals(
        (double) record.getSomeIntArray().stream().mapToInt(Integer::intValue).min().getAsInt(),

        //return type of min is double
        flattener.makeJsonPathExtractor("$.someIntArray.min()").apply(record)
    );
    Assert.assertEquals(
        (double) record.getSomeIntArray().stream().mapToInt(Integer::intValue).max().getAsInt(),

        //return type of max is double
        flattener.makeJsonPathExtractor("$.someIntArray.max()").apply(record)
    );
    Assert.assertEquals(
        record.getSomeIntArray().stream().mapToInt(Integer::intValue).average().getAsDouble(),
        flattener.makeJsonPathExtractor("$.someIntArray.avg()").apply(record)
    );
    Assert.assertEquals(
        record.getSomeIntArray().size(),
        flattener.makeJsonPathExtractor("$.someIntArray.length()").apply(record)
    );
    Assert.assertEquals(
        (double) record.getSomeIntArray().stream().mapToInt(Integer::intValue).sum(),

        //return type of sum is double
        flattener.makeJsonPathExtractor("$.someIntArray.sum()").apply(record)
    );
    Assert.assertEquals(
        2.681,
        (double) flattener.makeJsonPathExtractor("$.someIntArray.stddev()").apply(record),
        0.0001
    );
    Assert.assertEquals(
        record.getSomeStringArray(),
        flattener.makeJsonPathExtractor("$.someStringArray").apply(record)
    );
    Assert.assertEquals(
        record.getSomeIntValueMap(),
        flattener.makeJsonPathExtractor("$.someIntValueMap").apply(record)
    );
    Assert.assertEquals(
        record.getSomeStringValueMap(),
        flattener.makeJsonPathExtractor("$.someStringValueMap").apply(record)
    );
    Assert.assertEquals(
        record.getSomeUnion(),
        flattener.makeJsonPathExtractor("$.someUnion").apply(record)
    );
    Assert.assertEquals(
        record.getSomeNull(),
        flattener.makeJsonPathExtractor("$.someNull").apply(record)
    );
    Assert.assertEquals(
        // Casted to an array by transformValue
        record.getSomeFixed().bytes(),
        flattener.makeJsonPathExtractor("$.someFixed").apply(record)
    );
    Assert.assertEquals(
        // Casted to an array by transformValue
        record.getSomeBytes().array(),
        flattener.makeJsonPathExtractor("$.someBytes").apply(record)
    );
    Assert.assertEquals(
        // Casted to a string by transformValue
        record.getSomeEnum().toString(),
        flattener.makeJsonPathExtractor("$.someEnum").apply(record)
    );
    Map<String, Object> map = new HashMap<>();
    record.getSomeRecord()
          .getSchema()
          .getFields()
          .forEach(field -> map.put(field.name(), record.getSomeRecord().get(field.name())));
    Assert.assertEquals(
        map,
        flattener.makeJsonPathExtractor("$.someRecord").apply(record)
    );
    Assert.assertEquals(
        record.getSomeLong(),
        flattener.makeJsonPathExtractor("$.someLong").apply(record)
    );
    Assert.assertEquals(
        record.getSomeInt(),
        flattener.makeJsonPathExtractor("$.someInt").apply(record)
    );
    Assert.assertEquals(
        record.getSomeFloat(),
        flattener.makeJsonPathExtractor("$.someFloat").apply(record)
    );

    List<Map<String, Object>> list = new ArrayList<>();
    for (GenericRecord genericRecord : record.getSomeRecordArray()) {
      Map<String, Object> map1 = new HashMap<>();
      genericRecord
          .getSchema()
          .getFields()
          .forEach(field -> map1.put(field.name(), genericRecord.get(field.name())));
      list.add(map1);
    }

    Assert.assertEquals(
        list,
        flattener.makeJsonPathExtractor("$.someRecordArray").apply(record)
    );

    Assert.assertEquals(
        record.getSomeRecordArray().get(0).getNestedString(),
        flattener.makeJsonPathExtractor("$.someRecordArray[0].nestedString").apply(record)
    );

    Assert.assertEquals(
        list,
        flattener.makeJsonPathExtractor("$.someRecordArray[?(@.nestedString)]").apply(record)
    );

    List<String> nestedStringArray = Collections.singletonList(record.getSomeRecordArray().get(0).getNestedString().toString());
    Assert.assertEquals(
        nestedStringArray,
        flattener.makeJsonPathExtractor("$.someRecordArray[?(@.nestedString=='string in record')].nestedString").apply(record)
    );
  }
}
