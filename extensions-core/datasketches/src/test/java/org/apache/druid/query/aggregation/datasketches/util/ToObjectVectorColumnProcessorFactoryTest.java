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

package org.apache.druid.query.aggregation.datasketches.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

public class ToObjectVectorColumnProcessorFactoryTest extends InitializedNullHandlingTest
{
  private CursorFactory cursorFactory;

  @Before
  public void setUp()
  {
    final QueryableIndex index = TestIndex.getMMappedTestIndex();
    cursorFactory = new QueryableIndexCursorFactory(index);
  }

  @Test
  public void testRead()
  {
    try (final CursorHolder cursorHolder = makeCursorHolder()) {
      final VectorCursor cursor = cursorHolder.asVectorCursor();
      final Supplier<Object[]> qualitySupplier = ColumnProcessors.makeVectorProcessor(
          "quality",
          ToObjectVectorColumnProcessorFactory.INSTANCE,
          cursor.getColumnSelectorFactory()
      );

      final Supplier<Object[]> qualityLongSupplier = ColumnProcessors.makeVectorProcessor(
          "qualityLong",
          ToObjectVectorColumnProcessorFactory.INSTANCE,
          cursor.getColumnSelectorFactory()
      );

      final Supplier<Object[]> qualityFloatSupplier = ColumnProcessors.makeVectorProcessor(
          "qualityFloat",
          ToObjectVectorColumnProcessorFactory.INSTANCE,
          cursor.getColumnSelectorFactory()
      );

      final Supplier<Object[]> qualityDoubleSupplier = ColumnProcessors.makeVectorProcessor(
          "qualityDouble",
          ToObjectVectorColumnProcessorFactory.INSTANCE,
          cursor.getColumnSelectorFactory()
      );

      final Supplier<Object[]> placementSupplier = ColumnProcessors.makeVectorProcessor(
          "placement",
          ToObjectVectorColumnProcessorFactory.INSTANCE,
          cursor.getColumnSelectorFactory()
      );

      final Supplier<Object[]> qualityUniquesSupplier = ColumnProcessors.makeVectorProcessor(
          "quality_uniques",
          ToObjectVectorColumnProcessorFactory.INSTANCE,
          cursor.getColumnSelectorFactory()
      );
    }
  }

  @Test
  public void testString()
  {
    Assert.assertEquals(
        Arrays.asList(
            "automotive",
            "business",
            "entertainment",
            "health",
            "mezzanine",
            "news",
            "premium",
            "technology",
            "travel",
            "mezzanine"
        ),
        readColumn("quality", 10)
    );
  }

  @Test
  public void testLong()
  {
    Assert.assertEquals(
        Arrays.asList(1000L, 1100L, 1200L, 1300L, 1400L, 1500L, 1600L, 1700L, 1800L, 1400L),
        readColumn("qualityLong", 10)
    );
  }

  @Test
  public void testFloat()
  {
    Assert.assertEquals(
        Arrays.asList(10000f, 11000f, 12000f, 13000f, 14000f, 15000f, 16000f, 17000f, 18000f, 14000f),
        readColumn("qualityFloat", 10)
    );
  }

  @Test
  public void testDouble()
  {
    Assert.assertEquals(
        Arrays.asList(10000.0, 11000.0, 12000.0, 13000.0, 14000.0, 15000.0, 16000.0, 17000.0, 18000.0, 14000.0),
        readColumn("qualityDouble", 10)
    );
  }

  @Test
  public void testMultiString()
  {
    Assert.assertEquals(
        Arrays.asList(
            Arrays.asList("a", "preferred"),
            Arrays.asList("b", "preferred"),
            Arrays.asList("e", "preferred"),
            Arrays.asList("h", "preferred"),
            Arrays.asList("m", "preferred"),
            Arrays.asList("n", "preferred"),
            Arrays.asList("p", "preferred"),
            Arrays.asList("preferred", "t"),
            Arrays.asList("preferred", "t"),
            Arrays.asList("m", "preferred")
        ),
        readColumn("placementish", 10)
    );
  }

  @Test
  public void testComplexSketch()
  {
    final Object sketch = Iterables.getOnlyElement(readColumn("quality_uniques", 1));
    Assert.assertThat(sketch, CoreMatchers.instanceOf(HyperLogLogCollector.class));
  }

  private CursorHolder makeCursorHolder()
  {
    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
                                                     .setQueryContext(
                                                         QueryContext.of(
                                                             ImmutableMap.of(QueryContexts.VECTOR_SIZE_KEY, 3)
                                                         )
                                                     )
                                                     .build();
    return cursorFactory.makeCursorHolder(buildSpec);
  }

  private List<Object> readColumn(final String column, final int limit)
  {
    try (final CursorHolder cursorHolder = makeCursorHolder()) {
      final VectorCursor cursor = cursorHolder.asVectorCursor();
      final Supplier<Object[]> supplier = ColumnProcessors.makeVectorProcessor(
          column,
          ToObjectVectorColumnProcessorFactory.INSTANCE,
          cursor.getColumnSelectorFactory()
      );

      final List<Object> retVal = new ArrayList<>();

      while (!cursor.isDone()) {
        final Object[] objects = supplier.get();

        for (int i = 0; i < cursor.getCurrentVectorSize(); i++) {
          retVal.add(objects[i]);

          if (retVal.size() >= limit) {
            return retVal;
          }
        }

        cursor.advance();
      }

      return retVal;
    }
  }
}
