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

package org.apache.druid.uint;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.groupby.ResultRow;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class UnsignedIntAggregationTest extends DruidBaseTest
{

  private File segmentDir;

  @Before
  public void setUp() throws Exception
  {
    segmentDir = tempFolder.newFolder();
  }

  @After
  public void tearDown()
  {
    segmentDir.delete();
  }

  @Test
  public void testSumAgg() throws Exception
  {
    createSegmentFromDataFile(groupByTestHelper, "u_int_data.data", segmentDir);
    Sequence<ResultRow> sequence = groupByTestHelper.runQueryOnSegments(
        ImmutableList.of(segmentDir),
        readJsonAsString("queries/group_by_sum_query.json")
    );

    List<ResultRow> actual = sequence.toList();
    List<ResultRow> expected = Lists.newArrayList(
        ResultRow.of("a", 199L),
        ResultRow.of("b", 98L),
        ResultRow.of("c", (long) 2 * Integer.MAX_VALUE) // if only int agg, it would overflow.
    );
    assertEquals(3, actual.size());
    assertArrayEquals(expected.toArray(), actual.toArray());
  }

  @Test
  public void testMaxAgg() throws Exception
  {
    createSegmentFromDataFile(groupByTestHelper, "u_int_data.data", segmentDir);
    Sequence<ResultRow> sequence = groupByTestHelper.runQueryOnSegments(
        ImmutableList.of(segmentDir),
        readJsonAsString("queries/group_by_max_query.json")
    );

    List<ResultRow> actual = sequence.toList();
    List<ResultRow> expected = Lists.newArrayList(
        ResultRow.of("a", 100L),
        ResultRow.of("b", 97L),
        ResultRow.of("c", (long) Integer.MAX_VALUE) // if only int agg, it would overflow.
    );
    assertEquals(3, actual.size());
    assertArrayEquals(expected.toArray(), actual.toArray());
  }

  @Test
  public void testMinAgg() throws Exception
  {
    createSegmentFromDataFile(groupByTestHelper, "u_int_data.data", segmentDir);
    Sequence<ResultRow> sequence = groupByTestHelper.runQueryOnSegments(
        ImmutableList.of(segmentDir),
        readJsonAsString("queries/group_by_min_query.json")
    );

    List<ResultRow> actual = sequence.toList();
    List<ResultRow> expected = Lists.newArrayList(
        ResultRow.of("a", 99L),
        ResultRow.of("b", 1L),
        ResultRow.of("c", (long) Integer.MAX_VALUE) // if only int agg, it would overflow.
    );
    assertEquals(3, actual.size());
    assertArrayEquals(expected.toArray(), actual.toArray());
  }

}
