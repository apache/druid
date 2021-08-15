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

package org.apache.druid.sql.calcite.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.List;

@RunWith(Parameterized.class)
public class SegmentsTableBenchmarkQueryTest extends SegmentsTableBenchamrkBase
{
  private static final SettableSupplier<Boolean> FORCE_HASH_BASED_MERGE_SUPPLIER = new SettableSupplier<>(false);

  private static final String AVAILABLE_SEGMENTS_INTERVAL = "2021-01-01/P1Y";
  private static final String PUBLISHED_SEGMENTS_INTERVAL = "2021-01-02/P1Y";
  private static final String SEGMENT_GRANULARITY = "DAY";
  private static final int NUM_SEGMENTS_PER_INTERVAL = 100;

  @BeforeClass
  public static void setup()
  {
    PlannerConfig plannerConfig = new PlannerConfig()
    {
      @Override
      public boolean isMetadataSegmentCacheEnable()
      {
        return true;
      }

      @Override
      public boolean isForceHashBasedMergeForSegmentsTable()
      {
        return FORCE_HASH_BASED_MERGE_SUPPLIER.get();
      }

      @Override
      public long getMetadataSegmentPollPeriod()
      {
        return 1000;
      }
    };
    setupBenchmark(
        plannerConfig,
        SEGMENT_GRANULARITY,
        AVAILABLE_SEGMENTS_INTERVAL,
        PUBLISHED_SEGMENTS_INTERVAL,
        NUM_SEGMENTS_PER_INTERVAL
    );
  }

  @AfterClass
  public static void tearDown() throws IOException
  {
    tearDownBenchmark();
  }

  @Parameterized.Parameters(name = "query = {0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{0},
        new Object[]{1}
    );
  }

  private final int query;

  public SegmentsTableBenchmarkQueryTest(int query)
  {
    this.query = query;
  }

  @Test
  public void testSegmentsTableQuery() throws ValidationException, SqlParseException, RelConversionException
  {
    String sql = QUERIES.get(query);

    try (final DruidPlanner planner = plannerFactory.createPlannerForTesting(ImmutableMap.of(), sql)) {
      FORCE_HASH_BASED_MERGE_SUPPLIER.set(true);
      final PlannerResult plannerResultForHash = planner.plan(sql);
      final List<Object[]> resultBasedHash = plannerResultForHash.run().toList();

      FORCE_HASH_BASED_MERGE_SUPPLIER.set(false);
      final PlannerResult plannerResultForSort = planner.plan(sql);
      final List<Object[]> resultBasedSort = plannerResultForSort.run().toList();

      Assert.assertEquals(resultBasedHash.size(), resultBasedSort.size());
      for (int i = 0; i < resultBasedHash.size(); i++) {
        Assert.assertArrayEquals(resultBasedHash.get(i), resultBasedSort.get(i));
      }
    }
  }
}
