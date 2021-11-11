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

package org.apache.druid.timeline.partition;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.java.util.common.ISE;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class PartitionBoundariesTest
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private PartitionBoundaries target;
  private StringTuple[] values;
  private List<StringTuple> expected;

  @Before
  public void setup()
  {
    values = new StringTuple[]{
        StringTuple.create("a"),
        StringTuple.create("dup"),
        StringTuple.create("dup"),
        StringTuple.create("z")
    };
    expected = Arrays.asList(null, StringTuple.create("dup"), null);
    target = new PartitionBoundaries(values);
  }

  @Test
  public void hasCorrectValues()
  {
    Assert.assertEquals(expected, target);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void isImmutable()
  {
    target.add(StringTuple.create("should fail"));
  }

  @Test
  public void cannotBeIndirectlyModified()
  {
    values[1] = StringTuple.create("changed");
    Assert.assertEquals(expected, target);
  }

  @Test
  public void handlesNoValues()
  {
    Assert.assertEquals(Collections.emptyList(), new PartitionBoundaries());
  }

  @Test
  public void handlesRepeatedValue()
  {
    Assert.assertEquals(
        Arrays.asList(null, null),
        new PartitionBoundaries(
            StringTuple.create("a"),
            StringTuple.create("a"),
            StringTuple.create("a")
        )
    );
  }

  @Test
  public void serializesDeserializes()
  {
    String serialized = serialize(target);
    Object deserialized = deserialize(serialized, target.getClass());
    Assert.assertEquals(serialized, serialize(deserialized));
  }

  @Test
  public void testSerdeWithMultiDimensions()
  {
    PartitionBoundaries original = new PartitionBoundaries(
        StringTuple.create("a", "10"),
        StringTuple.create("b", "7"),
        StringTuple.create("c", "4")
    );

    String json = serialize(original);
    PartitionBoundaries deserialized = deserialize(json, PartitionBoundaries.class);
    Assert.assertEquals(original, deserialized);
  }

  @Test
  public void testGetSerializableObject_withMultiDimensions()
  {
    // Create a PartitionBoundaries for multiple dimensions
    PartitionBoundaries multiDimBoundaries = new PartitionBoundaries(
        StringTuple.create("a", "10"),
        StringTuple.create("b", "7"),
        StringTuple.create("c", "4")
    );

    // Verify that the serializable object is a List<StringTuple>
    Object serializableObject = multiDimBoundaries.getSerializableObject();
    Assert.assertTrue(serializableObject instanceof List);
    assertThatItemsAreNullOr(StringTuple.class, (List<?>) serializableObject);

    // Verify the output of getSerializableObject can be serialized/deserialized
    String json = serialize(serializableObject);
    PartitionBoundaries deserialized = deserialize(json, PartitionBoundaries.class);
    Assert.assertEquals(multiDimBoundaries, deserialized);
  }

  @Test
  public void testGetSerializableObject_withSingleDimension()
  {
    // Create a PartitionBoundaries for a single dimension
    PartitionBoundaries singleDimBoundaries = new PartitionBoundaries(
        StringTuple.create("a"),
        StringTuple.create("b"),
        StringTuple.create("c")
    );

    // Verify that the serializable object is a List<String>
    Object serializableObject = singleDimBoundaries.getSerializableObject();
    Assert.assertTrue(serializableObject instanceof List);
    assertThatItemsAreNullOr(String.class, (List<?>) serializableObject);

    // Verify the output of getSerializableObject can be serialized/deserialized
    String json = serialize(serializableObject);
    PartitionBoundaries deserialized = deserialize(json, PartitionBoundaries.class);
    Assert.assertEquals(singleDimBoundaries, deserialized);
  }

  @Test
  public void testDeserializeArrayOfString()
  {
    String json = "[null, \"a\", null]";
    PartitionBoundaries deserialized = deserialize(json, PartitionBoundaries.class);
    Assert.assertEquals(
        new PartitionBoundaries(
            null,
            StringTuple.create("a"),
            StringTuple.create("b")
        ),
        deserialized
    );
  }

  @Test
  public void testDeserializeArrayOfTuples()
  {
    String json = "[null, [\"a\",\"10\"], null]";
    PartitionBoundaries deserialized = deserialize(json, PartitionBoundaries.class);
    Assert.assertEquals(
        new PartitionBoundaries(
            null,
            StringTuple.create("a", "10"),
            StringTuple.create("a", "20")
        ),
        deserialized
    );
  }

  @Test
  public void testGetNumBucketsOfNonEmptyPartitionBoundariesReturningCorrectSize()
  {
    Assert.assertEquals(2, target.getNumBuckets());
  }

  @Test
  public void testEqualsContract()
  {
    EqualsVerifier.forClass(PartitionBoundaries.class).withNonnullFields("delegate").usingGetClass().verify();
  }

  /**
   * Asserts that all the items in the given list are either null or of the
   * specified class.
   */
  private <T> void assertThatItemsAreNullOr(Class<T> clazz, List<?> list)
  {
    if (list == null || list.isEmpty()) {
      return;
    }

    for (Object item : list) {
      if (item != null) {
        Assert.assertSame(clazz, item.getClass());
      }
    }
  }

  private String serialize(Object object)
  {
    try {
      return OBJECT_MAPPER.writeValueAsString(object);
    }
    catch (Exception e) {
      throw new ISE("Error while serializing");
    }
  }

  private <T> T deserialize(String json, Class<T> clazz)
  {
    try {
      return OBJECT_MAPPER.readValue(json, clazz);
    }
    catch (Exception e) {
      throw new ISE(e, "Error while deserializing");
    }
  }
}
