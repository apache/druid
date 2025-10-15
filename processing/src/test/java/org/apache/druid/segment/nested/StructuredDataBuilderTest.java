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

import org.apache.druid.error.DruidException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class StructuredDataBuilderTest
{
  @Test
  public void testBuildSingleDepth()
  {
    Object[] array = new Object[]{1, 2};
    StructuredDataBuilder.Element childArrayElement = new StructuredDataBuilder.Element(
        List.of(new NestedPathArrayElement(1)),
        array,
        1
    );
    Assert.assertEquals(StructuredData.wrap(array), new StructuredDataBuilder(childArrayElement).build());

    // [null, [1, 2]]
    StructuredDataBuilder.Element arrayElement = new StructuredDataBuilder.Element(
        List.of(new NestedPathArrayElement(1)),
        array,
        0
    );
    Assert.assertEquals(
        StructuredData.wrap(Arrays.asList(null, array)),
        new StructuredDataBuilder(arrayElement).build()
    );

    StructuredDataBuilder.Element nullElement = new StructuredDataBuilder.Element(
        List.of(new NestedPathField("y")),
        null,
        0
    );
    Assert.assertEquals(StructuredData.wrap(Map.of()), new StructuredDataBuilder(nullElement).build());

    // {"x": "hi"}
    StructuredDataBuilder.Element mapElement = new StructuredDataBuilder.Element(
        List.of(new NestedPathField("x")),
        "hi",
        0
    );
    Assert.assertEquals(
        StructuredData.wrap(Map.of("x", "hi")),
        new StructuredDataBuilder(mapElement, nullElement).build()
    );
  }

  @Test
  public void testBuildRootPath()
  {
    // "root-val"
    StructuredDataBuilder.Element rootElement = new StructuredDataBuilder.Element(
        List.of(),
        "root-val",
        0
    );
    Assert.assertEquals(StructuredData.wrap("root-val"), new StructuredDataBuilder(rootElement).build());
  }

  @Test
  public void testBuildArrayMultipleDepths()
  {
    // [[1], [null, [2]]]
    Object[] array = new Object[]{2};
    StructuredDataBuilder.Element element1 = new StructuredDataBuilder.Element(
        List.of(new NestedPathArrayElement(0), new NestedPathArrayElement(0)),
        1,
        0
    );
    StructuredDataBuilder.Element element2 = new StructuredDataBuilder.Element(
        List.of(new NestedPathArrayElement(1), new NestedPathArrayElement(1)),
        array,
        0
    );
    List<Object> expected = List.of(List.of(1), Arrays.asList(null, array));
    Assert.assertEquals(StructuredData.wrap(expected), new StructuredDataBuilder(element1, element2).build());
  }

  @Test
  public void testBuildMapMultipleDepths()
  {
    // {"x": {"y": "hi-xy", "z": "hi-xz"}, "yz": {"z": "hi-yz"}}
    StructuredDataBuilder.Element xyElement = new StructuredDataBuilder.Element(
        List.of(new NestedPathField("x"), new NestedPathField("y")),
        "hi-xy",
        0
    );
    StructuredDataBuilder.Element xzElement = new StructuredDataBuilder.Element(
        List.of(new NestedPathField("x"), new NestedPathField("z")),
        "hi-xz",
        0
    );
    StructuredDataBuilder.Element yzElement = new StructuredDataBuilder.Element(
        List.of(new NestedPathField("yz"), new NestedPathField("z")),
        "hi-yz",
        0
    );
    Map<String, Object> expected = Map.of("x", Map.of("y", "hi-xy", "z", "hi-xz"), "yz", Map.of("z", "hi-yz"));
    Assert.assertEquals(
        StructuredData.wrap(expected),
        new StructuredDataBuilder(xyElement, xzElement, yzElement).build()
    );
  }

  @Test
  public void testBuildMixedMultipleDepths()
  {
    // {"x": {"y": "hi-xy", "array": ["hi-x-array-0", null, "hi-x-array-2"]}}
    StructuredDataBuilder.Element xyElement = new StructuredDataBuilder.Element(
        List.of(new NestedPathField("x"), new NestedPathField("y")),
        "hi-xy",
        0
    );
    StructuredDataBuilder.Element xArray = new StructuredDataBuilder.Element(
        List.of(new NestedPathField("x"), new NestedPathField("array"), new NestedPathArrayElement(0)),
        "hi-x-array-0",
        0
    );
    StructuredDataBuilder.Element xArray2 = new StructuredDataBuilder.Element(
        List.of(new NestedPathField("x"), new NestedPathField("array"), new NestedPathArrayElement(2)),
        "hi-x-array-2",
        0
    );

    Map<String, Object> expected = Map.of(
        "x",
        Map.of("y", "hi-xy", "array", Arrays.asList("hi-x-array-0", null, "hi-x-array-2"))
    );
    Assert.assertEquals(StructuredData.wrap(expected), new StructuredDataBuilder(xyElement, xArray, xArray2).build());
  }

  @Test
  public void testBuildExceptions()
  {
    StructuredDataBuilder.Element rootElement = new StructuredDataBuilder.Element(
        List.of(),
        "root-val",
        0
    );
    StructuredDataBuilder.Element mapElement = new StructuredDataBuilder.Element(
        List.of(new NestedPathField("x")),
        "hi",
        0
    );
    StructuredDataBuilder.Element arrayElement = new StructuredDataBuilder.Element(
        List.of(new NestedPathArrayElement(0)),
        1,
        0
    );
    DruidException e1 = Assert.assertThrows(
        DruidException.class,
        () -> new StructuredDataBuilder(rootElement, mapElement).build()
    );
    Assert.assertEquals(
        "Error building structured data from paths[[Element{path=[], value=root-val, depth=0}, Element{path=[NestedPathField{field='x'}], value=hi, depth=0}]], "
        + "cannot have map or array elements when root value is set",
        e1.getMessage()
    );
    DruidException e2 = Assert.assertThrows(
        DruidException.class,
        () -> new StructuredDataBuilder(rootElement, arrayElement).build()
    );
    Assert.assertEquals(
        "Error building structured data from paths[[Element{path=[], value=root-val, depth=0}, Element{path=[NestedPathArrayElement{index=0}], value=1, depth=0}]], "
        + "cannot have map or array elements when root value is set",
        e2.getMessage()
    );
    DruidException e3 = Assert.assertThrows(
        DruidException.class,
        () -> new StructuredDataBuilder(mapElement, arrayElement).build()
    );
    Assert.assertEquals(
        "Error building structured data from paths[[Element{path=[NestedPathField{field='x'}], value=hi, depth=0}, Element{path=[NestedPathArrayElement{index=0}], value=1, depth=0}]], "
        + "cannot have both map and array elements at the same level",
        e3.getMessage()
    );
  }
}
