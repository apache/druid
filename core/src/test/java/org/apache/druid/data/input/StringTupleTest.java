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
import static org.junit.Assert.assertNull;

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
}
