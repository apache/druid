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

package org.apache.druid.frame.write;

import org.apache.druid.frame.allocation.ArenaMemoryAllocatorFactory;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.frame.write.columnar.ColumnarFrameWriterFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.filter.AllNullColumnSelectorFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import java.util.Collections;

/**
 * Tests {@link FrameWriters#makeRowBasedFrameWriterFactory} and {@link FrameWriters#makeColumnBasedFrameWriterFactory} ability to create factories.
 * Largely doesn't test actual frame generation via the factories, since that is exercised well enough in other test suites.
 */
public class FrameWritersTest extends InitializedNullHandlingTest
{
  private static final int ALLOCATOR_CAPACITY = 1000;

  @Test
  public void test_rowBased()
  {
    final FrameWriterFactory factory = FrameWriters.makeRowBasedFrameWriterFactory(
        new ArenaMemoryAllocatorFactory(ALLOCATOR_CAPACITY),
        RowSignature.builder().add("x", ColumnType.LONG).build(),
        Collections.singletonList(new KeyColumn("x", KeyOrder.ASCENDING)),
        false
    );

    MatcherAssert.assertThat(factory, CoreMatchers.instanceOf(RowBasedFrameWriterFactory.class));
    Assert.assertEquals(ALLOCATOR_CAPACITY, factory.allocatorCapacity());
  }

  @Test
  public void test_columnar()
  {
    final FrameWriterFactory factory = FrameWriters.makeColumnBasedFrameWriterFactory(
        new ArenaMemoryAllocatorFactory(ALLOCATOR_CAPACITY),
        RowSignature.builder()
                    .add("a", ColumnType.LONG)
                    .add("b", ColumnType.FLOAT)
                    .add("c", ColumnType.DOUBLE)
                    .add("d", ColumnType.STRING)
                    .add("e", ColumnType.LONG_ARRAY)
                    .add("f", ColumnType.FLOAT_ARRAY)
                    .add("g", ColumnType.DOUBLE_ARRAY)
                    .add("h", ColumnType.STRING_ARRAY)
                    .build(),
        Collections.emptyList()
    );

    MatcherAssert.assertThat(factory, CoreMatchers.instanceOf(ColumnarFrameWriterFactory.class));
    Assert.assertEquals(ALLOCATOR_CAPACITY, factory.allocatorCapacity());
  }

  @Test
  public void test_columnar_unsupportedColumnType()
  {
    final FrameWriterFactory factory = FrameWriters.makeColumnBasedFrameWriterFactory(
        new ArenaMemoryAllocatorFactory(ALLOCATOR_CAPACITY),
        RowSignature.builder().add("x", ColumnType.ofArray(ColumnType.LONG_ARRAY)).build(),
        Collections.emptyList()
    );

    final UnsupportedColumnTypeException e = Assert.assertThrows(
        UnsupportedColumnTypeException.class,
        () -> factory.newFrameWriter(new AllNullColumnSelectorFactory())
    );

    Assert.assertEquals("x", e.getColumnName());
    Assert.assertEquals(ColumnType.ofArray(ColumnType.LONG_ARRAY), e.getColumnType());
  }

  @Test
  public void test_rowBased_sortColumnsNotPrefix()
  {
    final IllegalArgumentException e = Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            FrameWriters.makeRowBasedFrameWriterFactory(
                new ArenaMemoryAllocatorFactory(ALLOCATOR_CAPACITY),
                RowSignature.builder().add("x", ColumnType.LONG).add("y", ColumnType.LONG).build(),
                Collections.singletonList(new KeyColumn("y", KeyOrder.ASCENDING)),
                false
            )
    );

    MatcherAssert.assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(
            CoreMatchers.containsString("Sort column [y] must be a prefix of the signature")
        )
    );
  }

  @Test
  public void test_columnar_cantSort()
  {
    final IllegalArgumentException e = Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            FrameWriters.makeColumnBasedFrameWriterFactory(
                new ArenaMemoryAllocatorFactory(ALLOCATOR_CAPACITY),
                RowSignature.builder().add("x", ColumnType.LONG).build(),
                Collections.singletonList(new KeyColumn("x", KeyOrder.ASCENDING))
            )
    );

    MatcherAssert.assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("Columnar frames cannot be sorted"))
    );
  }
}
