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
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.join.table.IndexedTableJoinable;
import org.apache.druid.timeline.SegmentId;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

public class HashJoinSegmentTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private QueryableIndexSegment baseSegment;
  private HashJoinSegment hashJoinSegmentNoClauses;
  private HashJoinSegment hashJoinSegmentManyClauses;

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

    hashJoinSegmentNoClauses = new HashJoinSegment(
        baseSegment,
        ImmutableList.of()
    );

    hashJoinSegmentManyClauses = new HashJoinSegment(
        baseSegment,
        ImmutableList.of(
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
        )
    );
  }

  @Test
  public void test_getId_noClauses()
  {
    Assert.assertEquals(baseSegment.getId(), hashJoinSegmentNoClauses.getId());
  }

  @Test
  public void test_getId_manyClauses()
  {
    Assert.assertEquals(baseSegment.getId(), hashJoinSegmentManyClauses.getId());
  }

  @Test
  public void test_getDataInterval_noClauses()
  {
    Assert.assertEquals(baseSegment.getDataInterval(), hashJoinSegmentNoClauses.getDataInterval());
  }

  @Test
  public void test_getDataInterval_manyClauses()
  {
    Assert.assertEquals(baseSegment.getDataInterval(), hashJoinSegmentManyClauses.getDataInterval());
  }

  @Test
  public void test_asQueryableIndex_noClauses()
  {
    Assert.assertNull(hashJoinSegmentNoClauses.asQueryableIndex());
  }

  @Test
  public void test_asQueryableIndex_manyClauses()
  {
    Assert.assertNull(hashJoinSegmentManyClauses.asQueryableIndex());
  }

  @Test
  public void test_asStorageAdapter_noClauses()
  {
    Assert.assertThat(
        hashJoinSegmentNoClauses.asStorageAdapter(),
        CoreMatchers.instanceOf(HashJoinSegmentStorageAdapter.class)
    );
  }

  @Test
  public void test_asStorageAdapter_manyClauses()
  {
    Assert.assertThat(
        hashJoinSegmentManyClauses.asStorageAdapter(),
        CoreMatchers.instanceOf(HashJoinSegmentStorageAdapter.class)
    );
  }
}
