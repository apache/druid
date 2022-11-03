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

package org.apache.druid.java.util.common.parsers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class ObjectFlattenersTest
{
  private static final String SOME_JSON = "{\"foo\": null, \"bar\": 1}";
  private static final ObjectFlattener FLATTENER = ObjectFlatteners.create(
      new JSONPathSpec(
          true,
          ImmutableList.of(new JSONPathFieldSpec(JSONPathFieldType.PATH, "extract", "$.bar"))
      ),
      new JSONFlattenerMaker(true)
  );
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testFlatten() throws JsonProcessingException
  {
    JsonNode node = OBJECT_MAPPER.readTree(SOME_JSON);
    Map<String, Object> flat = FLATTENER.flatten(node);
    Assert.assertEquals(ImmutableSet.of("extract", "foo", "bar"), flat.keySet());
    Assert.assertFalse(flat.isEmpty());
    Assert.assertNull(flat.get("foo"));
    Assert.assertEquals(1L, flat.get("bar"));
    Assert.assertEquals(1L, flat.get("extract"));
    Assert.assertEquals("{\"extract\":1,\"foo\":null,\"bar\":1}", OBJECT_MAPPER.writeValueAsString(flat));
  }

  @Test
  public void testToMap() throws JsonProcessingException
  {
    JsonNode node = OBJECT_MAPPER.readTree(SOME_JSON);
    Map<String, Object> flat = FLATTENER.toMap(node);
    Assert.assertNull(flat.get("foo"));
    Assert.assertEquals(1, flat.get("bar"));
  }
}
