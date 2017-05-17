/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.monomorphicprocessing;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public class StringRuntimeShapeTest
{
  static class Empty implements HotLoopCallee
  {
    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      // Don't care about runtime shape in tests
    }
  }

  static class Foo implements HotLoopCallee
  {
    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("flag1", true);
      inspector.visit("flag2", false);
      inspector.visit("key", "value");
      inspector.visit("empty", new Empty());
      inspector.visit("object", ByteBuffer.allocate(1));
      inspector.visit("array", new Set[] {new HashSet(), new TreeSet()});
      inspector.visit("emptyArray", new Set[] {});
    }
  }

  static class Bar implements HotLoopCallee
  {
    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("foo", new Foo());
      inspector.visit("array", new Foo[] {new Foo(), new Foo()});
    }
  }

  @Test
  public void testStringRuntimeShape()
  {
    String barRuntimeShape = StringRuntimeShape.of(new Bar());
    Assert.assertEquals(
        "io.druid.query.monomorphicprocessing.StringRuntimeShapeTest$Bar {\n"
        + "  foo: io.druid.query.monomorphicprocessing.StringRuntimeShapeTest$Foo {\n"
        + "    flag1: true,\n"
        + "    flag2: false,\n"
        + "    key: value,\n"
        + "    empty: io.druid.query.monomorphicprocessing.StringRuntimeShapeTest$Empty,\n"
        + "    object: java.nio.HeapByteBuffer {order: BIG_ENDIAN},\n"
        + "    array: [\n"
        + "      java.util.HashSet,\n"
        + "      java.util.TreeSet\n"
        + "    ],\n"
        + "    emptyArray: []\n"
        + "  },\n"
        + "  array: [\n"
        + "    io.druid.query.monomorphicprocessing.StringRuntimeShapeTest$Foo {\n"
        + "      flag1: true,\n"
        + "      flag2: false,\n"
        + "      key: value,\n"
        + "      empty: io.druid.query.monomorphicprocessing.StringRuntimeShapeTest$Empty,\n"
        + "      object: java.nio.HeapByteBuffer {order: BIG_ENDIAN},\n"
        + "      array: [\n"
        + "        java.util.HashSet,\n"
        + "        java.util.TreeSet\n"
        + "      ],\n"
        + "      emptyArray: []\n"
        + "    },\n"
        + "    io.druid.query.monomorphicprocessing.StringRuntimeShapeTest$Foo {\n"
        + "      flag1: true,\n"
        + "      flag2: false,\n"
        + "      key: value,\n"
        + "      empty: io.druid.query.monomorphicprocessing.StringRuntimeShapeTest$Empty,\n"
        + "      object: java.nio.HeapByteBuffer {order: BIG_ENDIAN},\n"
        + "      array: [\n"
        + "        java.util.HashSet,\n"
        + "        java.util.TreeSet\n"
        + "      ],\n"
        + "      emptyArray: []\n"
        + "    }\n"
        + "  ]\n"
        + "}",
        barRuntimeShape
    );
  }
}
