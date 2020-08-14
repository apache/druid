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

package org.apache.druid.segment.join;

import com.google.common.collect.ImmutableList;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.join.table.IndexedTableJoinable;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Optional;

public class InlineJoinableFactoryTest
{
  private static final String PREFIX = "j.";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final InlineJoinableFactory factory = new InlineJoinableFactory();

  private final InlineDataSource inlineDataSource = InlineDataSource.fromIterable(
      ImmutableList.of(
          new Object[]{"foo", 1L},
          new Object[]{"bar", 2L}
      ),
      RowSignature.builder().add("str", ValueType.STRING).add("long", ValueType.LONG).build()
  );

  @Test
  public void testBuildNonInline()
  {
    expectedException.expect(ClassCastException.class);
    expectedException.expectMessage("TableDataSource cannot be cast");

    final Optional<Joinable> ignored = factory.build(new TableDataSource("foo"), makeCondition("x == \"j.y\""));
  }

  @Test
  public void testBuildNonHashJoin()
  {
    Assert.assertEquals(
        Optional.empty(),
        factory.build(inlineDataSource, makeCondition("x > \"j.y\""))
    );
  }

  @Test
  public void testBuild()
  {
    final Joinable joinable = factory.build(inlineDataSource, makeCondition("x == \"j.long\"")).get();

    Assert.assertThat(joinable, CoreMatchers.instanceOf(IndexedTableJoinable.class));
    Assert.assertEquals(ImmutableList.of("str", "long"), joinable.getAvailableColumns());
    Assert.assertEquals(3, joinable.getCardinality("str"));
    Assert.assertEquals(3, joinable.getCardinality("long"));
  }

  @Test
  public void testIsDirectlyJoinable()
  {
    Assert.assertTrue(factory.isDirectlyJoinable(inlineDataSource));
    Assert.assertFalse(factory.isDirectlyJoinable(new TableDataSource("foo")));
  }

  private static JoinConditionAnalysis makeCondition(final String condition)
  {
    return JoinConditionAnalysis.forExpression(condition, PREFIX, ExprMacroTable.nil());
  }
}
