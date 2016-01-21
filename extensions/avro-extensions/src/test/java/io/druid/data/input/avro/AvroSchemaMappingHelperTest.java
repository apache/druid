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

import org.junit.Test;

import java.util.List;

import io.druid.data.input.avro.PathComponent.PathComponentType;

import static org.junit.Assert.assertEquals;

/**
 * 
 */
public class AvroSchemaMappingHelperTest
{

  @Test
  public void testParseSchemaMapping()
  {
    List<PathComponent> list = AvroSchemaMappingHelper.parseSchemaMapping("A.B[0].D(C)");
    
    assertEquals(5, list.size());
    assertEquals(PathComponentType.FIELD, list.get(0).getType());
    assertEquals("A", list.get(0).getFieldName());
    assertEquals(PathComponentType.FIELD, list.get(1).getType());
    assertEquals("B", list.get(1).getFieldName());
    assertEquals(PathComponentType.ARRAY, list.get(2).getType());
    assertEquals(0, list.get(2).getArrayIdx().intValue());
    assertEquals(PathComponentType.FIELD, list.get(3).getType());
    assertEquals("D", list.get(3).getFieldName());
    assertEquals(PathComponentType.MAP, list.get(4).getType());
    assertEquals("C", list.get(4).getMapKey());
  }
  
  @Test (expected=IllegalArgumentException.class)
  public void testParseSchemaMappingMismatch()
  {
    @SuppressWarnings("unused")
    List<PathComponent> list = AvroSchemaMappingHelper.parseSchemaMapping("A[0)");
  }

  @Test (expected=IllegalArgumentException.class)
  public void testParseSchemaMappingMismatch2()
  {
    @SuppressWarnings("unused")
    List<PathComponent> list = AvroSchemaMappingHelper.parseSchemaMapping("A(0]");
  }
  
  @Test(expected=NumberFormatException.class)
  public void testParseSchemaMappingBadArrayIdx ()
  {
    @SuppressWarnings("unused")
    List<PathComponent> list = AvroSchemaMappingHelper.parseSchemaMapping("A[B]");
  }

  @Test
  public void testSimpleFieldAccessor()
  {
    List<PathComponent> list = AvroSchemaMappingHelper.getSimpleFieldAccessor("hello");
    
    assertEquals(1, list.size());
    assertEquals(PathComponentType.FIELD, list.get(0).getType());
    assertEquals("hello", list.get(0).getFieldName());
  }
}
