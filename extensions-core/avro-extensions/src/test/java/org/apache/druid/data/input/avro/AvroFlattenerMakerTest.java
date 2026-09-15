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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.druid.data.input.AvroStreamInputFormatTest;
import org.apache.druid.data.input.SomeAvroDatum;
import org.apache.druid.data.input.UnionSubEnum;
import org.apache.druid.data.input.UnionSubFixed;
import org.apache.druid.data.input.UnionSubRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class AvroFlattenerMakerTest
{
  private static final AvroFlattenerMaker FLATTENER_WITHOUT_EXTRACT_UNION_BY_TYPE =
      new AvroFlattenerMaker(false, false, false, false);
  private static final AvroFlattenerMaker FLATTENER_WITH_EXTRACT_UNION_BY_TYPE =
      new AvroFlattenerMaker(false, false, true, false);

  private static final SomeAvroDatum RECORD = AvroStreamInputFormatTest.buildSomeAvroDatum();

  public static SomeAvroDatum buildSomeAvroDatumWithUnionValue(Object unionValue)
  {
    return AvroStreamInputFormatTest.createSomeAvroDatumBuilderDefaults()
                                    .setSomeMultiMemberUnion(unionValue)
                                    .build();
  }

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
    Assertions.assertEquals(
        RECORD.getSomeMultiMemberUnion(),
        FLATTENER_WITH_EXTRACT_UNION_BY_TYPE.makeJsonPathExtractor("$.someMultiMemberUnion.int").apply(RECORD)
    );
  }

  @Test
  public void jsonPathExtractorExtractUnionsByType()
  {
    final AvroFlattenerMaker flattener = new AvroFlattenerMaker(false, false, true, false);

    // Unmamed types are accessed by type

    // int
    Assertions.assertEquals(1, flattener.makeJsonPathExtractor("$.someMultiMemberUnion.int").apply(
        buildSomeAvroDatumWithUnionValue(1)));

    // long
    Assertions.assertEquals(1L, flattener.makeJsonPathExtractor("$.someMultiMemberUnion.long").apply(
        buildSomeAvroDatumWithUnionValue(1L)));

    // float
    Assertions.assertEquals((float) 1.0, flattener.makeJsonPathExtractor("$.someMultiMemberUnion.float").apply(
        buildSomeAvroDatumWithUnionValue((float) 1.0)));

    // double
    Assertions.assertEquals(1.0, flattener.makeJsonPathExtractor("$.someMultiMemberUnion.double").apply(
        buildSomeAvroDatumWithUnionValue(1.0)));

    // string
    Assertions.assertEquals("string", flattener.makeJsonPathExtractor("$.someMultiMemberUnion.string").apply(
        buildSomeAvroDatumWithUnionValue(new Utf8("string"))));

    // bytes
    Assertions.assertArrayEquals(new byte[] {1}, (byte[]) flattener.makeJsonPathExtractor("$.someMultiMemberUnion.bytes").apply(
        buildSomeAvroDatumWithUnionValue(ByteBuffer.wrap(new byte[] {1}))));

    // map
    Assertions.assertEquals(2, flattener.makeJsonPathExtractor("$.someMultiMemberUnion.map.two").apply(
        buildSomeAvroDatumWithUnionValue(
            ImmutableMap.<String, Integer>builder()
                        .put("one", 1)
                        .put("two", 2)
                        .put("three", 3)
                        .build()
        )));

    // array
    Assertions.assertEquals(3, flattener.makeJsonPathExtractor("$.someMultiMemberUnion.array[2]").apply(
        buildSomeAvroDatumWithUnionValue(Arrays.asList(1, 2, 3))));

    // Named types are accessed by name

    // record
    Assertions.assertEquals("subRecordString", flattener.makeJsonPathExtractor("$.someMultiMemberUnion.UnionSubRecord.subString").apply(
        buildSomeAvroDatumWithUnionValue(
            UnionSubRecord.newBuilder()
                          .setSubString("subRecordString")
                          .build())));

    // fixed
    final byte[] fixedBytes = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    Assertions.assertEquals(fixedBytes, flattener.makeJsonPathExtractor("$.someMultiMemberUnion.UnionSubFixed").apply(
        buildSomeAvroDatumWithUnionValue(new UnionSubFixed(fixedBytes))));

    // enum
    Assertions.assertEquals(String.valueOf(UnionSubEnum.ENUM1), flattener.makeJsonPathExtractor("$.someMultiMemberUnion.UnionSubEnum").apply(
        buildSomeAvroDatumWithUnionValue(UnionSubEnum.ENUM1)));
  }

  @Test
  public void makeJsonQueryExtractor_flattenerWithoutExtractUnionsByType()
  {
    assertThrows(UnsupportedOperationException.class, () ->
      Assertions.assertEquals(
          RECORD.getTimestamp(),
          FLATTENER_WITHOUT_EXTRACT_UNION_BY_TYPE.makeJsonQueryExtractor("$.timestamp").apply(RECORD)
      ));
  }

  @Test
  public void makeJsonQueryExtractor_flattenerWithExtractUnionsByType()
  {
    assertThrows(UnsupportedOperationException.class, () ->
      Assertions.assertEquals(
          RECORD.getTimestamp(),
          FLATTENER_WITH_EXTRACT_UNION_BY_TYPE.makeJsonQueryExtractor("$.timestamp").apply(RECORD)
      ));
  }

  @Test
  public void testDiscovery()
  {
    final AvroFlattenerMaker flattener = new AvroFlattenerMaker(false, false, true, false);
    final AvroFlattenerMaker flattenerNested = new AvroFlattenerMaker(false, false, true, true);

    SomeAvroDatum input = AvroStreamInputFormatTest.buildSomeAvroDatum();
    // isFieldPrimitive on someStringArray is false
    // as it contains items as nulls and strings
    // so flattenerNested should only be able to discover it
    Assertions.assertEquals(
        ImmutableSet.of(
            "someOtherId",
            "someIntArray",
            "someFloat",
            "eventType",
            "someFixed",
            "someBytes",
            "someUnion",
            "id",
            "someEnum",
            "someLong",
            "someInt",
            "timestamp"
        ),
        ImmutableSet.copyOf(flattener.discoverRootFields(input))
    );
    Assertions.assertEquals(
        ImmutableSet.of(
            "someStringValueMap",
            "someOtherId",
            "someStringArray",
            "someIntArray",
            "someFloat",
            "isValid",
            "someIntValueMap",
            "eventType",
            "someFixed",
            "someBytes",
            "someRecord",
            "someMultiMemberUnion",
            "someNull",
            "someRecordArray",
            "someUnion",
            "id",
            "someEnum",
            "someLong",
            "someInt",
            "timestamp"
        ),
        ImmutableSet.copyOf(flattenerNested.discoverRootFields(input))
    );
  }


  @Test
  public void testNullsInStringArray()
  {
    final AvroFlattenerMaker flattenerNested = new AvroFlattenerMaker(false, false, true, true);

    SomeAvroDatum input = AvroStreamInputFormatTest.buildSomeAvroDatum();

    Assertions.assertEquals(
        ImmutableSet.of(
            "someStringValueMap",
            "someOtherId",
            "someStringArray",
            "someIntArray",
            "someFloat",
            "isValid",
            "someIntValueMap",
            "eventType",
            "someFixed",
            "someBytes",
            "someRecord",
            "someMultiMemberUnion",
            "someNull",
            "someRecordArray",
            "someUnion",
            "id",
            "someEnum",
            "someLong",
            "someInt",
            "timestamp"
        ),
        ImmutableSet.copyOf(flattenerNested.discoverRootFields(input))
    );

    ArrayList<Object> results = (ArrayList<Object>) flattenerNested.getRootField(input, "someStringArray");
    // 4 strings a 1 null for a total of 5
    Assertions.assertEquals("8", results.get(0).toString());
    Assertions.assertEquals("4", results.get(1).toString());
    Assertions.assertEquals("2", results.get(2).toString());
    Assertions.assertEquals("1", results.get(3).toString());
    Assertions.assertEquals(null, results.get(4));
  }

  private void getRootField_common(final SomeAvroDatum record, final AvroFlattenerMaker flattener)
  {
    Assertions.assertEquals(
        record.getTimestamp(),
        flattener.getRootField(record, "timestamp")
    );
    Assertions.assertEquals(
        record.getEventType(),
        flattener.getRootField(record, "eventType")
    );
    Assertions.assertEquals(
        record.getId(),
        flattener.getRootField(record, "id")
    );
    Assertions.assertEquals(
        record.getSomeOtherId(),
        flattener.getRootField(record, "someOtherId")
    );
    Assertions.assertEquals(
        record.getIsValid(),
        flattener.getRootField(record, "isValid")
    );
    Assertions.assertEquals(
        record.getSomeIntArray(),
        flattener.getRootField(record, "someIntArray")
    );
    Assertions.assertEquals(
        record.getSomeStringArray(),
        flattener.getRootField(record, "someStringArray")
    );
    Assertions.assertEquals(
        record.getSomeIntValueMap(),
        flattener.getRootField(record, "someIntValueMap")
    );
    Assertions.assertEquals(
        record.getSomeStringValueMap(),
        flattener.getRootField(record, "someStringValueMap")
    );
    Assertions.assertEquals(
        record.getSomeUnion(),
        flattener.getRootField(record, "someUnion")
    );
    Assertions.assertEquals(
        record.getSomeNull(),
        flattener.getRootField(record, "someNull")
    );
    Assertions.assertEquals(
        // Casted to an array by transformValue
        record.getSomeFixed().bytes(),
        flattener.getRootField(record, "someFixed")
    );
    Assertions.assertEquals(
        // Casted to an array by transformValue
        record.getSomeBytes().array(),
        flattener.getRootField(record, "someBytes")
    );
    Assertions.assertEquals(
        // Casted to a string by transformValue
        record.getSomeEnum().toString(),
        flattener.getRootField(record, "someEnum")
    );
    Map<String, Object> map = new HashMap<>();
    record.getSomeRecord()
          .getSchema()
          .getFields()
          .forEach(field -> map.put(field.name(), record.getSomeRecord().get(field.name())));
    Assertions.assertEquals(
        map,
        flattener.getRootField(record, "someRecord")
    );
    Assertions.assertEquals(
        record.getSomeLong(),
        flattener.getRootField(record, "someLong")
    );
    Assertions.assertEquals(
        record.getSomeInt(),
        flattener.getRootField(record, "someInt")
    );
    Assertions.assertEquals(
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
    Assertions.assertEquals(
        list,
        flattener.getRootField(record, "someRecordArray")
    );
    Assertions.assertEquals(
        null,
        flattener.getRootField(record, "invalidField")
    );
  }

  private void makeJsonPathExtractor_common(final SomeAvroDatum record, final AvroFlattenerMaker flattener)
  {
    Assertions.assertEquals(
        record.getTimestamp(),
        flattener.makeJsonPathExtractor("$.timestamp").apply(record)
    );
    Assertions.assertEquals(
        record.getEventType(),
        flattener.makeJsonPathExtractor("$.eventType").apply(record)
    );
    Assertions.assertEquals(
        record.getId(),
        flattener.makeJsonPathExtractor("$.id").apply(record)
    );
    Assertions.assertEquals(
        record.getSomeOtherId(),
        flattener.makeJsonPathExtractor("$.someOtherId").apply(record)
    );
    Assertions.assertEquals(
        record.getIsValid(),
        flattener.makeJsonPathExtractor("$.isValid").apply(record)
    );
    Assertions.assertEquals(
        record.getSomeIntArray(),
        flattener.makeJsonPathExtractor("$.someIntArray").apply(record)
    );
    Assertions.assertEquals(
        (double) record.getSomeIntArray().stream().mapToInt(Integer::intValue).min().getAsInt(),

        //return type of min is double
        flattener.makeJsonPathExtractor("$.someIntArray.min()").apply(record)
    );
    Assertions.assertEquals(
        (double) record.getSomeIntArray().stream().mapToInt(Integer::intValue).max().getAsInt(),

        //return type of max is double
        flattener.makeJsonPathExtractor("$.someIntArray.max()").apply(record)
    );
    Assertions.assertEquals(
        record.getSomeIntArray().stream().mapToInt(Integer::intValue).average().getAsDouble(),
        flattener.makeJsonPathExtractor("$.someIntArray.avg()").apply(record)
    );
    Assertions.assertEquals(
        record.getSomeIntArray().size(),
        flattener.makeJsonPathExtractor("$.someIntArray.length()").apply(record)
    );
    Assertions.assertEquals(
        (double) record.getSomeIntArray().stream().mapToInt(Integer::intValue).sum(),

        //return type of sum is double
        flattener.makeJsonPathExtractor("$.someIntArray.sum()").apply(record)
    );
    Assertions.assertEquals(
        2.681,
        (double) flattener.makeJsonPathExtractor("$.someIntArray.stddev()").apply(record),
        0.0001
    );
    Assertions.assertEquals(
        record.getSomeStringArray(),
        flattener.makeJsonPathExtractor("$.someStringArray").apply(record)
    );
    Assertions.assertEquals(
        record.getSomeIntValueMap(),
        flattener.makeJsonPathExtractor("$.someIntValueMap").apply(record)
    );
    Assertions.assertEquals(
        record.getSomeStringValueMap(),
        flattener.makeJsonPathExtractor("$.someStringValueMap").apply(record)
    );
    Assertions.assertEquals(
        record.getSomeUnion(),
        flattener.makeJsonPathExtractor("$.someUnion").apply(record)
    );
    Assertions.assertEquals(
        record.getSomeNull(),
        flattener.makeJsonPathExtractor("$.someNull").apply(record)
    );
    Assertions.assertEquals(
        // Casted to an array by transformValue
        record.getSomeFixed().bytes(),
        flattener.makeJsonPathExtractor("$.someFixed").apply(record)
    );
    Assertions.assertEquals(
        // Casted to an array by transformValue
        record.getSomeBytes().array(),
        flattener.makeJsonPathExtractor("$.someBytes").apply(record)
    );
    Assertions.assertEquals(
        // Casted to a string by transformValue
        record.getSomeEnum().toString(),
        flattener.makeJsonPathExtractor("$.someEnum").apply(record)
    );
    Map<String, Object> map = new HashMap<>();
    record.getSomeRecord()
          .getSchema()
          .getFields()
          .forEach(field -> map.put(field.name(), record.getSomeRecord().get(field.name())));
    Assertions.assertEquals(
        map,
        flattener.makeJsonPathExtractor("$.someRecord").apply(record)
    );
    Assertions.assertEquals(
        record.getSomeLong(),
        flattener.makeJsonPathExtractor("$.someLong").apply(record)
    );
    Assertions.assertEquals(
        record.getSomeInt(),
        flattener.makeJsonPathExtractor("$.someInt").apply(record)
    );
    Assertions.assertEquals(
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

    Assertions.assertEquals(
        list,
        flattener.makeJsonPathExtractor("$.someRecordArray").apply(record)
    );

    Assertions.assertEquals(
        record.getSomeRecordArray().get(0).getNestedString(),
        flattener.makeJsonPathExtractor("$.someRecordArray[0].nestedString").apply(record)
    );

    Assertions.assertEquals(
        list,
        flattener.makeJsonPathExtractor("$.someRecordArray[?(@.nestedString)]").apply(record)
    );

    List<String> nestedStringArray = Collections.singletonList(record.getSomeRecordArray().get(0).getNestedString().toString());
    Assertions.assertEquals(
        nestedStringArray,
        flattener.makeJsonPathExtractor("$.someRecordArray[?(@.nestedString=='string in record')].nestedString").apply(record)
    );
  }
}
