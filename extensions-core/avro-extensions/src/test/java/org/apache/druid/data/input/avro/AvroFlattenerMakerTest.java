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

import org.apache.avro.util.Utf8;
import org.apache.druid.data.input.AvroStreamInputRowParserTest;
import org.apache.druid.data.input.SomeAvroDatum;
import org.apache.druid.data.input.UnionSubEnum;
import org.apache.druid.data.input.UnionSubFixed;
import org.apache.druid.data.input.UnionSubRecord;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class AvroFlattenerMakerTest
{

  @Test
  public void getRootField()
  {
    final SomeAvroDatum record = AvroStreamInputRowParserTest.buildSomeAvroDatum();
    final AvroFlattenerMaker flattener = new AvroFlattenerMaker(false, false, true);

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
    Assert.assertEquals(
        record.getSomeRecord(),
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
    Assert.assertEquals(
        record.getSomeRecordArray(),
        flattener.getRootField(record, "someRecordArray")
    );
  }

  @Test
  public void makeJsonPathExtractor()
  {
    final SomeAvroDatum record = AvroStreamInputRowParserTest.buildSomeAvroDatum();
    final AvroFlattenerMaker flattener = new AvroFlattenerMaker(false, false, true);

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
        record.getSomeMultiMemberUnion(),
        flattener.makeJsonPathExtractor("$.someMultiMemberUnion.int").apply(record)
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
    Assert.assertEquals(
        record.getSomeRecord(),
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
    Assert.assertEquals(
        record.getSomeRecordArray(),
        flattener.makeJsonPathExtractor("$.someRecordArray").apply(record)
    );

    Assert.assertEquals(
        record.getSomeRecordArray().get(0).getNestedString(),
        flattener.makeJsonPathExtractor("$.someRecordArray[0].nestedString").apply(record)
    );

    Assert.assertEquals(
        record.getSomeRecordArray(),
        flattener.makeJsonPathExtractor("$.someRecordArray[?(@.nestedString)]").apply(record)
    );

    List<String> nestedStringArray = Collections.singletonList(record.getSomeRecordArray().get(0).getNestedString().toString());
    Assert.assertEquals(
        nestedStringArray,
        flattener.makeJsonPathExtractor("$.someRecordArray[?(@.nestedString=='string in record')].nestedString").apply(record)
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
  public void makeJsonQueryExtractor()
  {
    final SomeAvroDatum record = AvroStreamInputRowParserTest.buildSomeAvroDatum();
    final AvroFlattenerMaker flattener = new AvroFlattenerMaker(false, false, false);

    Assert.assertEquals(
        record.getTimestamp(),
        flattener.makeJsonQueryExtractor("$.timestamp").apply(record)
    );
  }
}
