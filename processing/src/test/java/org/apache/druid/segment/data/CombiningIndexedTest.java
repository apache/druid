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

package org.apache.druid.segment.data;

import com.google.common.collect.ImmutableList;
import org.apache.commons.compress.utils.Lists;
import org.apache.druid.query.monomorphicprocessing.HotLoopCallee;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CombiningIndexedTest
{
  private final List<List<Integer>> delegateLists;
  private final Indexed<Integer> target;

  public CombiningIndexedTest()
  {
    delegateLists = ImmutableList.of(
        IntStream.range(0, 7).boxed().collect(Collectors.toList()),
        IntStream.range(7, 9).boxed().collect(Collectors.toList()),
        IntStream.range(9, 13).boxed().collect(Collectors.toList())
    );
    target = new CombiningIndexed<>(delegateLists.stream().map(ListIndexed::new).collect(Collectors.toList()));
  }

  @Test
  public void testSize()
  {
    Assert.assertEquals(delegateLists.stream().mapToInt(List::size).sum(), target.size());
  }

  @Test
  public void testIterator()
  {
    Assert.assertEquals(
        delegateLists.stream().flatMap(List::stream).collect(Collectors.toList()),
        Lists.newArrayList(target.iterator())
    );
  }

  @Test
  public void testGet()
  {
    int idx = 0;
    int start;
    for (List<Integer> delegateList : delegateLists) {
      start = idx;
      for (; idx - start < delegateList.size(); idx++) {
        Assert.assertEquals(delegateList.get(idx - start), target.get(idx));
      }
    }
  }

  @Test
  public void testGetOutOfBounds()
  {
    Assert.assertNull(target.get(-1));
    Assert.assertNull(target.get(Integer.MAX_VALUE));
  }

  @Test
  public void testIndexOf()
  {
    int startIdx = 0;
    int idx = 0;
    for (List<Integer> delegateList : delegateLists) {
      for (Integer i : delegateList) {
        Assert.assertEquals(startIdx + idx++, target.indexOf(i));
      }
    }
  }

  @Test
  public void testIndexOfUnknownValues()
  {
    Assert.assertEquals(-1, target.indexOf(null));
    Assert.assertEquals(-1, target.indexOf(-1));
    Assert.assertEquals(-1, target.indexOf(100));
  }

  @Test
  public void testInspectRuntimeShape()
  {
    class BaseListCountingRuntimeShapeInspector implements RuntimeShapeInspector
    {
      int baseListCount = 0;

      @Override
      public void visit(String fieldName, @Nullable HotLoopCallee value)
      {
      }

      @Override
      public void visit(String fieldName, @Nullable Object value)
      {
        if ("baseList".equals(fieldName)) {
          baseListCount++;
        }
      }

      @Override
      public <T> void visit(String fieldName, T[] values)
      {
      }

      @Override
      public void visit(String flagName, boolean flagValue)
      {
      }

      @Override
      public void visit(String key, String runtimeShape)
      {
      }
    }
    final BaseListCountingRuntimeShapeInspector runtimeShapeInspector = new BaseListCountingRuntimeShapeInspector();
    target.inspectRuntimeShape(runtimeShapeInspector);
    Assert.assertEquals(delegateLists.size(), runtimeShapeInspector.baseListCount);
  }
}
