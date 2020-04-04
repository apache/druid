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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.join.filter.JoinFilterAnalyzer;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysis;
import org.apache.druid.segment.join.table.IndexedTableJoinable;
import org.apache.druid.timeline.SegmentId;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;

public class HashJoinSegmentTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private QueryableIndexSegment baseSegment;
  private HashJoinSegment hashJoinSegment;

  @BeforeClass
  public static void setUpStatic()
  {
    NullHandling.initializeForTests();
  }

  @Before
  public void setUp() throws IOException
  {
    baseSegment = new QueryableIndexSegment(
        JoinTestHelper.createFactIndexBuilder(temporaryFolder.newFolder()).buildMMappedIndex(),
        SegmentId.dummy("facts")
    );

    List<JoinableClause> joinableClauses = ImmutableList.of(
        new JoinableClause(
            "j0.",
            new IndexedTableJoinable(JoinTestHelper.createCountriesIndexedTable()),
            JoinType.LEFT,
            JoinConditionAnalysis.forExpression("1", "j0.", ExprMacroTable.nil())
        ),
        new JoinableClause(
            "j1.",
            new IndexedTableJoinable(JoinTestHelper.createRegionsIndexedTable()),
            JoinType.LEFT,
            JoinConditionAnalysis.forExpression("1", "j1.", ExprMacroTable.nil())
        )
    );

    JoinFilterPreAnalysis joinFilterPreAnalysis = JoinFilterAnalyzer.computeJoinFilterPreAnalysis(
        joinableClauses,
        VirtualColumns.EMPTY,
        null,
        true,
        true,
        true,
        QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_MAX_SIZE_KEY
    );

    hashJoinSegment = new HashJoinSegment(
        baseSegment,
        joinableClauses,
        joinFilterPreAnalysis
    );
  }

  @Test
  public void test_constructor_noClauses()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("'clauses' is empty, no need to create HashJoinSegment");

    List<JoinableClause> joinableClauses = ImmutableList.of();

    JoinFilterPreAnalysis joinFilterPreAnalysis = JoinFilterAnalyzer.computeJoinFilterPreAnalysis(
        joinableClauses,
        VirtualColumns.EMPTY,
        null,
        true,
        true,
        true,
        QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_MAX_SIZE_KEY
    );

    final HashJoinSegment ignored = new HashJoinSegment(
        baseSegment,
        joinableClauses,
        joinFilterPreAnalysis
    );
  }

  @Test
  public void test_getId()
  {
    Assert.assertEquals(baseSegment.getId(), hashJoinSegment.getId());
  }

  @Test
  public void test_getDataInterval()
  {
    Assert.assertEquals(baseSegment.getDataInterval(), hashJoinSegment.getDataInterval());
  }

  @Test
  public void test_asQueryableIndex()
  {
    Assert.assertNull(hashJoinSegment.asQueryableIndex());
  }

  @Test
  public void test_asStorageAdapter()
  {
    Assert.assertThat(
        hashJoinSegment.asStorageAdapter(),
        CoreMatchers.instanceOf(HashJoinSegmentStorageAdapter.class)
    );
  }
}
