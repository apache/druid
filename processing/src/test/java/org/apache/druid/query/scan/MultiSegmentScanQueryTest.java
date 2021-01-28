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

package org.apache.druid.query.scan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.CharSource;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.druid.common.config.NullHandlingTest;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.MergeSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 *
 */
@RunWith(Parameterized.class)
public class MultiSegmentScanQueryTest extends NullHandlingTest
{
  private static final ScanQueryQueryToolChest TOOL_CHEST = new ScanQueryQueryToolChest(
      new ScanQueryConfig(),
      DefaultGenericQueryMetricsFactory.instance()
  );

  private static final QueryRunnerFactory<ScanResultValue, ScanQuery> FACTORY = new ScanQueryRunnerFactory(
      TOOL_CHEST,
      new ScanQueryEngine(),
      new ScanQueryConfig()
  );
  private static final Interval I_0112_0114 = Intervals.of("2011-01-12/2011-01-14");
  public static final QuerySegmentSpec I_0112_0114_SPEC = new LegacySegmentSpec(I_0112_0114);

  // time modified version of druid.sample.numeric.tsv
  public static final String[] V_0112 = {
      "2011-01-12T00:00:00.000Z\tspot\tautomotive\t1000\t10000.0\t10000.0\t100000\tpreferred\tapreferred\t100.000000",
      "2011-01-12T01:00:00.000Z\tspot\tbusiness\t1100\t11000.0\t11000.0\t110000\tpreferred\tbpreferred\t100.000000",
      "2011-01-12T02:00:00.000Z\tspot\tentertainment\t1200\t12000.0\t12000.0\t120000\tpreferred\tepreferred\t100.000000",
      "2011-01-12T03:00:00.000Z\tspot\thealth\t1300\t13000.0\t13000.0\t130000\tpreferred\thpreferred\t100.000000",
      "2011-01-12T04:00:00.000Z\tspot\tmezzanine\t1400\t14000.0\t14000.0\t140000\tpreferred\tmpreferred\t100.000000",
      "2011-01-12T05:00:00.000Z\tspot\tnews\t1500\t15000.0\t15000.0\t150000\tpreferred\tnpreferred\t100.000000",
      "2011-01-12T06:00:00.000Z\tspot\tpremium\t1600\t16000.0\t16000.0\t160000\tpreferred\tppreferred\t100.000000",
      "2011-01-12T07:00:00.000Z\tspot\ttechnology\t1700\t17000.0\t17000.0\t170000\tpreferred\ttpreferred\t100.000000",
      "2011-01-12T08:00:00.000Z\tspot\ttravel\t1800\t18000.0\t18000.0\t180000\tpreferred\ttpreferred\t100.000000",
      "2011-01-12T09:00:00.000Z\ttotal_market\tmezzanine\t1400\t14000.0\t14000.0\t140000\tpreferred\tmpreferred\t1000.000000",
      "2011-01-12T10:00:00.000Z\ttotal_market\tpremium\t1600\t16000.0\t16000.0\t160000\tpreferred\tppreferred\t1000.000000",
      "2011-01-12T11:00:00.000Z\tupfront\tmezzanine\t1400\t14000.0\t14000.0\t140000\tpreferred\tmpreferred\t800.000000\tvalue",
      "2011-01-12T12:00:00.000Z\tupfront\tpremium\t1600\t16000.0\t16000.0\t160000\tpreferred\tppreferred\t800.000000\tvalue"
  };

  public static final String[] V_0113 = {
      "2011-01-13T00:00:00.000Z\tspot\tautomotive\t1000\t10000.0\t10000.0\t100000\tpreferred\tapreferred\t94.874713",
      "2011-01-13T01:00:00.000Z\tspot\tbusiness\t1100\t11000.0\t11000.0\t110000\tpreferred\tbpreferred\t103.629399",
      "2011-01-13T02:00:00.000Z\tspot\tentertainment\t1200\t12000.0\t12000.0\t120000\tpreferred\tepreferred\t110.087299",
      "2011-01-13T03:00:00.000Z\tspot\thealth\t1300\t13000.0\t13000.0\t130000\tpreferred\thpreferred\t114.947403",
      "2011-01-13T04:00:00.000Z\tspot\tmezzanine\t1400\t14000.0\t14000.0\t140000\tpreferred\tmpreferred\t104.465767",
      "2011-01-13T05:00:00.000Z\tspot\tnews\t1500\t15000.0\t15000.0\t150000\tpreferred\tnpreferred\t102.851683",
      "2011-01-13T06:00:00.000Z\tspot\tpremium\t1600\t16000.0\t16000.0\t160000\tpreferred\tppreferred\t108.863011",
      "2011-01-13T07:00:00.000Z\tspot\ttechnology\t1700\t17000.0\t17000.0\t170000\tpreferred\ttpreferred\t111.356672",
      "2011-01-13T08:00:00.000Z\tspot\ttravel\t1800\t18000.0\t18000.0\t180000\tpreferred\ttpreferred\t106.236928",
      "2011-01-13T09:00:00.000Z\ttotal_market\tmezzanine\t1400\t14000.0\t14000.0\t140000\tpreferred\tmpreferred\t1040.945505",
      "2011-01-13T10:00:00.000Z\ttotal_market\tpremium\t1600\t16000.0\t16000.0\t160000\tpreferred\tppreferred\t1689.012875",
      "2011-01-13T11:00:00.000Z\tupfront\tmezzanine\t1400\t14000.0\t14000.0\t140000\tpreferred\tmpreferred\t826.060182\tvalue",
      "2011-01-13T12:00:00.000Z\tupfront\tpremium\t1600\t16000.0\t16000.0\t160000\tpreferred\tppreferred\t1564.617729\tvalue"
  };

  private static Segment segment0;
  private static Segment segment1;

  @BeforeClass
  public static void setup() throws IOException
  {
    CharSource v_0112 = CharSource.wrap(StringUtils.join(V_0112, "\n"));
    CharSource v_0113 = CharSource.wrap(StringUtils.join(V_0113, "\n"));

    IncrementalIndex index0 = TestIndex.loadIncrementalIndex(newIndex("2011-01-12T00:00:00.000Z"), v_0112);
    IncrementalIndex index1 = TestIndex.loadIncrementalIndex(newIndex("2011-01-13T00:00:00.000Z"), v_0113);

    segment0 = new IncrementalIndexSegment(index0, makeIdentifier(index0, "v1"));
    segment1 = new IncrementalIndexSegment(index1, makeIdentifier(index1, "v1"));
  }

  private static SegmentId makeIdentifier(IncrementalIndex index, String version)
  {
    return makeIdentifier(index.getInterval(), version);
  }

  private static SegmentId makeIdentifier(Interval interval, String version)
  {
    return SegmentId.of(QueryRunnerTestHelper.DATA_SOURCE, interval, version, NoneShardSpec.instance());
  }

  private static IncrementalIndex newIndex(String minTimeStamp)
  {
    return newIndex(minTimeStamp, 10000);
  }

  private static IncrementalIndex newIndex(String minTimeStamp, int maxRowCount)
  {
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(DateTimes.of(minTimeStamp).getMillis())
        .withQueryGranularity(Granularities.HOUR)
        .withMetrics(TestIndex.METRIC_AGGS)
        .build();
    return new OnheapIncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(maxRowCount)
        .build();
  }

  @AfterClass
  public static void clear()
  {
    IOUtils.closeQuietly(segment0);
    IOUtils.closeQuietly(segment1);
  }

  @Parameterized.Parameters(name = "limit={0},offset={1},batchSize={2}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return QueryRunnerTestHelper.cartesian(
        Arrays.asList(0, 1, 3, 7, 10, 20, 1000),
        Arrays.asList(0, 1, 3, 5, 7, 10, 20, 200, 1000),
        Arrays.asList(0, 1, 3, 6, 7, 10, 123, 2000)
    );
  }

  private final int limit;
  private final int offset;
  private final int batchSize;

  public MultiSegmentScanQueryTest(int limit, int offset, int batchSize)
  {
    this.limit = limit;
    this.offset = offset;
    this.batchSize = batchSize;
  }

  private Druids.ScanQueryBuilder newBuilder()
  {
    return Druids.newScanQueryBuilder()
                 .dataSource(new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE))
                 .intervals(I_0112_0114_SPEC)
                 .batchSize(batchSize)
                 .columns(Collections.emptyList())
                 .legacy(false)
                 .limit(limit)
                 .offset(offset);
  }

  @Test
  public void testMergeRunnersWithLimitAndOffset()
  {
    ScanQuery query = newBuilder().build();
    List<ScanResultValue> results = FACTORY
        .mergeRunners(
            Execs.directExecutor(),
            ImmutableList.of(FACTORY.createRunner(segment0), FACTORY.createRunner(segment1))
        )
        .run(QueryPlus.wrap(query))
        .toList();
    int totalCount = 0;
    for (ScanResultValue result : results) {
      System.out.println(((List) result.getEvents()).size());
      totalCount += ((List) result.getEvents()).size();
    }
    Assert.assertEquals(
        totalCount,
        limit != 0 ? Math.min(limit, V_0112.length + V_0113.length) : V_0112.length + V_0113.length
    );
  }

  @Test
  public void testMergeResultsWithLimitAndOffset()
  {
    QueryRunner<ScanResultValue> runner = TOOL_CHEST.mergeResults(
        (queryPlus, responseContext) -> {
          // simulate results back from 2 historicals
          List<Sequence<ScanResultValue>> sequences = Lists.newArrayListWithExpectedSize(2);
          sequences.add(FACTORY.createRunner(segment0).run(queryPlus));
          sequences.add(FACTORY.createRunner(segment1).run(queryPlus));
          return new MergeSequence<>(
              queryPlus.getQuery().getResultOrdering(),
              Sequences.simple(sequences)
          );
        }
    );
    ScanQuery query = newBuilder().build();
    List<ScanResultValue> results = runner.run(QueryPlus.wrap(query)).toList();
    int totalCount = 0;
    for (ScanResultValue result : results) {
      totalCount += ((List) result.getEvents()).size();
    }
    Assert.assertEquals(
        totalCount,
        Math.max(
            0,
            limit != 0
            ? Math.min(limit, V_0112.length + V_0113.length - offset)
            : V_0112.length + V_0113.length - offset
        )
    );
  }
}
