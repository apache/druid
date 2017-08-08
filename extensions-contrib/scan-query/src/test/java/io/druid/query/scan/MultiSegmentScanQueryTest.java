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

package io.druid.query.scan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.CharSource;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.MergeSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.DefaultGenericQueryMetricsFactory;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.TableDataSource;
import io.druid.query.select.SelectQueryRunnerTest;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.TestIndex;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class MultiSegmentScanQueryTest
{
  private static final ScanQueryQueryToolChest toolChest = new ScanQueryQueryToolChest(
      DefaultGenericQueryMetricsFactory.instance()
  );

  private static final QueryRunnerFactory<ScanResultValue, ScanQuery> factory = new ScanQueryRunnerFactory(
      toolChest,
      new ScanQueryEngine()
  );

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

  private static String makeIdentifier(IncrementalIndex index, String version)
  {
    return makeIdentifier(index.getInterval(), version);
  }

  private static String makeIdentifier(Interval interval, String version)
  {
    return DataSegment.makeDataSegmentIdentifier(
        QueryRunnerTestHelper.dataSource,
        interval.getStart(),
        interval.getEnd(),
        version,
        NoneShardSpec.instance()
    );
  }

  private static IncrementalIndex newIndex(String minTimeStamp)
  {
    return newIndex(minTimeStamp, 10000);
  }

  private static IncrementalIndex newIndex(String minTimeStamp, int maxRowCount)
  {
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(new DateTime(minTimeStamp).getMillis())
        .withQueryGranularity(Granularities.HOUR)
        .withMetrics(TestIndex.METRIC_AGGS)
        .build();
    return new IncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(maxRowCount)
        .buildOnheap();
  }

  @AfterClass
  public static void clear()
  {
    IOUtils.closeQuietly(segment0);
    IOUtils.closeQuietly(segment1);
  }

  @Parameterized.Parameters(name = "limit={0},batchSize={1}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.cartesian(Arrays.asList(0, 1, 3, 7, 10, 20, 1000), Arrays.asList(0, 1, 3, 6, 7, 10, 123, 2000));
  }

  private final int limit;
  private final int batchSize;

  public MultiSegmentScanQueryTest(int limit, int batchSize)
  {
    this.limit = limit;
    this.batchSize = batchSize;
  }

  private ScanQuery.ScanQueryBuilder newBuilder()
  {
    return ScanQuery.newScanQueryBuilder()
                    .dataSource(new TableDataSource(QueryRunnerTestHelper.dataSource))
                    .intervals(SelectQueryRunnerTest.I_0112_0114)
                    .batchSize(batchSize)
                    .columns(Arrays.<String>asList())
                    .limit(limit);
  }

  @Test
  public void testMergeRunnersWithLimit()
  {
    ScanQuery query = newBuilder().build();
    List<ScanResultValue> results = Sequences.toList(
        factory.mergeRunners(MoreExecutors.sameThreadExecutor(), ImmutableList.of(
            factory.createRunner(segment0),
            factory.createRunner(segment1)
        )).run(query, new HashMap<String, Object>()),
        Lists.<ScanResultValue>newArrayList()
    );
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
  public void testMergeResultsWithLimit()
  {
    QueryRunner<ScanResultValue> runner = toolChest.mergeResults(
        new QueryRunner<ScanResultValue>() {
          @Override
          public Sequence<ScanResultValue> run(
              QueryPlus<ScanResultValue> queryPlus, Map<String, Object> responseContext
          )
          {
            // simulate results back from 2 historicals
            List<Sequence<ScanResultValue>> sequences = Lists.newArrayListWithExpectedSize(2);
            sequences.add(factory.createRunner(segment0).run(queryPlus, new HashMap<String, Object>()));
            sequences.add(factory.createRunner(segment1).run(queryPlus, new HashMap<String, Object>()));
            return new MergeSequence<>(
                queryPlus.getQuery().getResultOrdering(),
                Sequences.simple(sequences)
            );
          }
        }
    );
    ScanQuery query = newBuilder().build();
    List<ScanResultValue> results = Sequences.toList(
        runner.run(query, new HashMap<String, Object>()),
        Lists.<ScanResultValue>newArrayList()
    );
    int totalCount = 0;
    for (ScanResultValue result : results) {
      totalCount += ((List) result.getEvents()).size();
    }
    Assert.assertEquals(
        totalCount,
        limit != 0 ? Math.min(limit, V_0112.length + V_0113.length) : V_0112.length + V_0113.length
    );
  }
}
