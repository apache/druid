/**
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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
package io.druid.data.input.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.StringType;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.hamcrest.collection.IsMapContaining;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.metamx.common.parsers.ParseException;
import io.druid.data.input.MyEnum;
import io.druid.data.input.MyFixed;
import io.druid.data.input.MySubRecord;
import io.druid.data.input.SomeAvroDatum;

import static io.druid.data.input.avro.PathComponent.PathComponentType.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class GenericRecordAsMapTest
{
  private static GenericRecord testSpecificRecord;
  private static GenericRecord testGenericRecord;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception
  {
    testSpecificRecord = SomeAvroDatum.newBuilder()
        .setTimestamp(1234567890)
        .setEventType("a")
        .setId(111)
        .setSomeOtherId(222)
        .setIsValid(true)
        .setSomeFloat(1234.0f)
        .setSomeInt(2345)
        .setSomeLong(4561237890L)
        .setSomeIntArray(Arrays.asList(1, 2, 3, 4))
        .setSomeStringArray(Arrays.<CharSequence> asList("1", "2", "3", "4"))
        .setSomeIntValueMap(
            new ImmutableMap.Builder<CharSequence, Integer>().put(new Utf8("a"), 1).put(new Utf8("b"), 4).build())
        .setSomeStringValueMap(new ImmutableMap.Builder<CharSequence, CharSequence>().put(new Utf8("a"), "A")
            .put(new Utf8("b"), "B").build())
        .setSomeUnion("union")
        .setSomeFixed(new MyFixed("fixed".getBytes()))
        .setSomeBytes(ByteBuffer.wrap("bytes".getBytes()))
        .setSomeNull(null)
        .setSomeEnum(MyEnum.ENUM1)
        .setSomeRecord(MySubRecord.newBuilder()
            .setSubInt(-1).setSubLong(-2)
            .setSubMap(
                new ImmutableMap.Builder<CharSequence, Integer>().put(new Utf8("a"), 10).put(new Utf8("b"), 40).build())
            .setSubArray(Arrays.<CharSequence> asList("11", "21", "31", "41"))
            .build())
        .build();

    Schema schema = new Schema.Parser().parse(GenericRecordAsMapTest.class.getResourceAsStream("/some-datum.avsc"));
    // for the generic record, set it so that it uses String instead of Utf8 for map keys and other string values.
    GenericData.setStringType(schema, StringType.String);
    testGenericRecord = new GenericRecordBuilder(schema)
        .set("timestamp", 1234567890)
        .set("eventType", "a")
        .set("id", 111)
        .set("someOtherId", 222)
        .set("isValid", false)
        .set("someFloat", 1234.0f)
        .set("someInt", 2345)
        .set("someLong", 4561237890L)
        .set("someIntArray", Arrays.<Integer> asList(1, 2, 3, 4))
        .set("someStringArray", Arrays.<CharSequence> asList("1", "2", "3", "4"))
        .set("someIntValueMap", new ImmutableMap.Builder<CharSequence, Integer>().put("a", 1).put("b", 4).build())
        .set("someStringValueMap",
            new ImmutableMap.Builder<CharSequence, CharSequence>().put("a", "A").put("b", "B").build())
        .set("someUnion", "union")
        .set("someFixed", "fixed".getBytes())
        .set("someBytes", ByteBuffer.wrap("bytes".getBytes()))
        .set("someNull", null)
        .set("someEnum", "ENUM1")
        .set("someRecord", new GenericRecordBuilder(schema.getField("someRecord").schema())
            .set("subInt", -1).set("subLong", -2)
            .set("subMap", new ImmutableMap.Builder<CharSequence, Integer>().put("a", 10).put("b", 40).build())
            .set("subArray", Arrays.<CharSequence> asList("11", "21", "31", "41"))
            .build())
        .build();
  }

  /**
   * Validate get() on basic fields (no mappings) on a specific avro object
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testGetSpecificBasicFields()
  {
    GenericRecordAsMap r = new GenericRecordAsMap(testSpecificRecord, false,
        Collections.<String, List<PathComponent>> emptyMap());

    // test normal fields
    assertEquals(1234567890L, r.get("timestamp"));
    assertEquals("a", r.get("eventType"));

    // test complex type
    assertThat((List<Integer>) r.get("someIntArray"), IsIterableContainingInOrder.contains(1, 2, 3, 4));
    assertThat((Map<Utf8, String>) r.get("someStringValueMap"), IsMapContaining.hasEntry(new Utf8("a"), "A"));

    // test missing fields
    assertNull(r.get("missingField"));
  }

  /**
   * Validate get() on basic fields (no mappings) on a specific avro object
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testGetGenericBasicFields()
  {
    GenericRecordAsMap r = new GenericRecordAsMap(testGenericRecord, false,
        Collections.<String, List<PathComponent>> emptyMap());

    // test normal fields
    assertEquals(1234567890, r.get("timestamp"));
    assertEquals("a", r.get("eventType"));

    // test complex type
    assertThat((List<Integer>) r.get("someIntArray"), IsIterableContainingInOrder.contains(1, 2, 3, 4));
    assertThat((Map<CharSequence, CharSequence>) r.get("someStringValueMap"),
        IsMapContaining.hasEntry((CharSequence) "a", (CharSequence) "A"));

    // test missing fields
    assertNull(r.get("missingField"));
  }

  @Test
  public void testGetSpecificMappedFields()
  {
    Map<String, List<PathComponent>> mappingCache = new ImmutableMap.Builder<String, List<PathComponent>>()
        .put("mapValue", Arrays.asList(new PathComponent(FIELD, "someIntValueMap"), new PathComponent(MAP, "a")))
        .put("arrayValue", Arrays.asList(new PathComponent(FIELD, "someIntArray"), new PathComponent(ARRAY, 2)))
        .put("missingField", Arrays.asList(new PathComponent(FIELD, "missingMap"), new PathComponent(MAP, "a")))
        .put("missingMapValue", Arrays.asList(new PathComponent(FIELD, "someIntValueMap"), new PathComponent(MAP, "q")))
        .put("missingArrayValue",
            Arrays.asList(new PathComponent(FIELD, "someIntArray"), new PathComponent(ARRAY, 100)))
        .build();
    GenericRecordAsMap r = new GenericRecordAsMap(testSpecificRecord, false, mappingCache);

    assertEquals(1, r.get("mapValue"));
    assertEquals(3, r.get("arrayValue"));

    // test missing fields
    assertNull(r.get("missingField"));
    assertNull(r.get("missingMapValue"));
    assertNull(r.get("missingArrayValue"));
  }

  @Test
  public void testGetGenericMappedFields()
  {
    Map<String, List<PathComponent>> mappingCache = new ImmutableMap.Builder<String, List<PathComponent>>()
        .put("mapValue", Arrays.asList(new PathComponent(FIELD, "someIntValueMap"), new PathComponent(MAP, "a")))
        .put("arrayValue", Arrays.asList(new PathComponent(FIELD, "someIntArray"), new PathComponent(ARRAY, 2)))
        .put("missingField", Arrays.asList(new PathComponent(FIELD, "missingMap"), new PathComponent(MAP, "a")))
        .put("missingMapValue", Arrays.asList(new PathComponent(FIELD, "someIntValueMap"), new PathComponent(MAP, "q")))
        .put("missingArrayValue",
            Arrays.asList(new PathComponent(FIELD, "someIntArray"), new PathComponent(ARRAY, 100)))
        .build();
    GenericRecordAsMap r = new GenericRecordAsMap(testSpecificRecord, false, mappingCache);

    assertEquals(1, r.get("mapValue"));
    assertEquals(3, r.get("arrayValue"));

    // test missing fields
    assertNull(r.get("missingField"));
    assertNull(r.get("missingMapValue"));
    assertNull(r.get("missingArrayValue"));
  }

  @Test(expected = ParseException.class)
  public void testInvalidArrayField()
  {
    Map<String, List<PathComponent>> mappingCache = new ImmutableMap.Builder<String, List<PathComponent>>()
        .put("arrayValue", Arrays.asList(new PathComponent(FIELD, "someIntValueMap"), new PathComponent(ARRAY, 1)))
        .build();
    GenericRecordAsMap r = new GenericRecordAsMap(testGenericRecord, false, mappingCache);
    r.get("arrayValue");
  }
  
  @Test(expected = ParseException.class)
  public void testInvalidMapField()
  {
    Map<String, List<PathComponent>> mappingCache = new ImmutableMap.Builder<String, List<PathComponent>>()
        .put("mapValue", Arrays.asList(new PathComponent(FIELD, "someIntArray"), new PathComponent(MAP, "a")))
        .build();
    GenericRecordAsMap r = new GenericRecordAsMap(testGenericRecord, false, mappingCache);
    r.get("mapValue");
  }

  @Test(expected = ParseException.class)
  public void testInvalidRecordField()
  {
    Map<String, List<PathComponent>> mappingCache = new ImmutableMap.Builder<String, List<PathComponent>>()
        .put("recValue", Arrays.asList(new PathComponent(FIELD, "someIntValueMap"), new PathComponent(FIELD, "dummy")))
        .build();
    GenericRecordAsMap r = new GenericRecordAsMap(testGenericRecord, false, mappingCache);
    r.get("recValue");
  }
}
