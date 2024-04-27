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

package org.apache.druid.data.input;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StringTupleTest
{

  @Test
  public void testSize()
  {
    StringTuple tuple = StringTuple.create("a", "b", "c");
    assertEquals(3, tuple.size());
  }

  @Test
  public void testGet()
  {
    StringTuple tuple = StringTuple.create("a", "b", "c");
    assertEquals("a", tuple.get(0));
    assertEquals("b", tuple.get(1));
    assertEquals("c", tuple.get(2));
  }

  @Test
  public void testToArray()
  {
    StringTuple tuple = StringTuple.create("a", "b", "c");
    assertEquals(new String[]{"a", "b", "c"}, tuple.toArray());
  }

  @Test
  public void testWithNullValues()
  {
    StringTuple tuple = StringTuple.create("a", null, "b");
    assertEquals("a", tuple.get(0));
    assertNull(tuple.get(1));
    assertEquals("b", tuple.get(2));

    tuple = StringTuple.create(null, null);
    assertNull(tuple.get(0));
    assertNull(tuple.get(1));

    tuple = StringTuple.create((String) null);
    assertNull(tuple.get(0));
  }

  @Test
  public void testSerde() throws IOException
  {
    StringTuple original = StringTuple.create("a", "b", "c");
    final ObjectMapper mapper = new ObjectMapper();
    String json = mapper.writeValueAsString(original);

    StringTuple deserialized = mapper.readValue(json, StringTuple.class);
    assertEquals(original, deserialized);
  }

  @Test
  public void testCompareTo()
  {
    StringTuple lhs = StringTuple.create("c", "10");

    // Objects equal to lhs
    assertEquals(
        0,
        lhs.compareTo(StringTuple.create("c", "10"))
    );

    // Objects smaller than lhs
    assertTrue(lhs.compareTo(null) > 0);
    assertTrue(lhs.compareTo(StringTuple.create(null, null)) > 0);
    assertTrue(lhs.compareTo(StringTuple.create("c", "09")) > 0);
    assertTrue(lhs.compareTo(StringTuple.create("b", "01")) > 0);

    // Objects bigger than lhs
    assertTrue(lhs.compareTo(StringTuple.create("c", "11")) < 0);
    assertTrue(lhs.compareTo(StringTuple.create("d", "01")) < 0);
  }

  @Test
  public void testEquals()
  {
    assertEquals(
        StringTuple.create((String) null),
        StringTuple.create((String) null)
    );
    assertEquals(
        StringTuple.create("a"),
        StringTuple.create("a")
    );
    assertEquals(
        StringTuple.create(null, null, null),
        StringTuple.create(null, null, null)
    );
    assertEquals(
        StringTuple.create("a", "10", "z"),
        StringTuple.create("a", "10", "z")
    );
    assertEquals(
        new StringTuple(new String[]{"a", "10", "z"}),
        StringTuple.create("a", "10", "z")
    );

    assertNotEquals(
        StringTuple.create(null, null, null),
        StringTuple.create(null, null)
    );
    assertNotEquals(
        StringTuple.create("a"),
        StringTuple.create((String) null)
    );
    assertNotEquals(
        StringTuple.create("a", "b"),
        StringTuple.create("a", "c")
    );
    assertNotEquals(
        StringTuple.create("a", "b"),
        StringTuple.create("c", "b")
    );
  }
}
