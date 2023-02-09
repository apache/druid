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

package org.apache.druid.segment.nested;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class NestedPathPartTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();
  private static final TypeReference<List<NestedPathPart>> TYPE_REF = new TypeReference<List<NestedPathPart>>()
  {
  };

  @Test
  public void testMapFieldSerde() throws JsonProcessingException
  {
    NestedPathPart fieldPart = new NestedPathField("x");
    String json = JSON_MAPPER.writeValueAsString(fieldPart);
    Assert.assertEquals(fieldPart, JSON_MAPPER.readValue(json, NestedPathPart.class));
  }

  @Test
  public void testArrayElementSerde() throws JsonProcessingException
  {
    NestedPathPart arrayElementPart = new NestedPathArrayElement(1);
    String json = JSON_MAPPER.writeValueAsString(arrayElementPart);
    Assert.assertEquals(arrayElementPart, JSON_MAPPER.readValue(json, NestedPathPart.class));
  }

  @Test
  public void testFieldEqualsAndHashCode()
  {
    EqualsVerifier.forClass(NestedPathField.class).usingGetClass().withNonnullFields("field").verify();
  }

  @Test
  public void testArrayElementEqualsAndHashCode()
  {
    EqualsVerifier.forClass(NestedPathArrayElement.class).usingGetClass().verify();
  }
}
