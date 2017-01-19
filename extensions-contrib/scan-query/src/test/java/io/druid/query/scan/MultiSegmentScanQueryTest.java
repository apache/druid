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
import io.druid.granularity.QueryGranularities;
import io.druid.java.util.common.guava.MergeSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Query;
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
import io.druid.segment.incremental.OnheapIncrementalIndex;
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
  private static final ScanQueryQueryToolChest toolChest = new ScanQueryQueryToolChest();

  private static final QueryRunnerFactory<ScanResultValue, ScanQuery> factory = new ScanQueryRunnerFactory(
      toolChest,
      new ScanQueryEngine()
  );

  // time modified version of druid.sample.tsv
  public static final String[] V_0112 = {
      "2011-01-12T00:00:00.000Z	spot	automotive	preferred	apreferred	100.000000",
      "2011-01-12T01:00:00.000Z	spot	business	preferred	bpreferred	100.000000",
      "2011-01-12T02:00:00.000Z	spot	entertainment	preferred	epreferred	100.000000",
      "2011-01-12T03:00:00.000Z	spot	health	preferred	hpreferred	100.000000",
      "2011-01-12T04:00:00.000Z	spot	mezzanine	preferred	mpreferred	100.000000",
      "2011-01-12T05:00:00.000Z	spot	news	preferred	npreferred	100.000000",
      "2011-01-12T06:00:00.000Z	spot	premium	preferred	ppreferred	100.000000",
      "2011-01-12T07:00:00.000Z	spot	technology	preferred	tpreferred	100.000000",
      "2011-01-12T08:00:00.000Z	spot	travel	preferred	tpreferred	100.000000",
      "2011-01-12T09:00:00.000Z	total_market	mezzanine	preferred	mpreferred	1000.000000",
      "2011-01-12T10:00:00.000Z	total_market	premium	preferred	ppreferred	1000.000000",
      "2011-01-12T11:00:00.000Z	upfront	mezzanine	preferred	mpreferred	800.000000	value",
      "2011-01-12T12:00:00.000Z	upfront	premium	preferred	ppreferred	800.000000	value",
      "2011-01-12T13:00:00.000Z	upfront	premium	preferred	ppreferred2	800.000000	value"
  };
  public static final String[] V_0113 = {
      "2011-01-13T00:00:00.000Z	spot	automotive	preferred	apreferred	94.874713",
      "2011-01-13T01:00:00.000Z	spot	business	preferred	bpreferred	103.629399",
      "2011-01-13T02:00:00.000Z	spot	entertainment	preferred	epreferred	110.087299",
      "2011-01-13T03:00:00.000Z	spot	health	preferred	hpreferred	114.947403",
      "2011-01-13T04:00:00.000Z	spot	mezzanine	preferred	mpreferred	104.465767",
      "2011-01-13T05:00:00.000Z	spot	news	preferred	npreferred	102.851683",
      "2011-01-13T06:00:00.000Z	spot	premium	preferred	ppreferred	108.863011",
      "2011-01-13T07:00:00.000Z	spot	technology	preferred	tpreferred	111.356672",
      "2011-01-13T08:00:00.000Z	spot	travel	preferred	tpreferred	106.236928",
      "2011-01-13T09:00:00.000Z	total_market	mezzanine	preferred	mpreferred	1040.945505",
      "2011-01-13T10:00:00.000Z	total_market	premium	preferred	ppreferred	1689.012875",
      "2011-01-13T11:00:00.000Z	upfront	mezzanine	preferred	mpreferred	826.060182	value",
      "2011-01-13T12:00:00.000Z	upfront	premium	preferred	ppreferred	1564.617729	value"
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
        .withQueryGranularity(QueryGranularities.HOUR)
        .withMetrics(TestIndex.METRIC_AGGS)
        .build();
    return new OnheapIncrementalIndex(schema, true, maxRowCount);
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
              Query<ScanResultValue> query, Map<String, Object> responseContext
          )
          {
            // simulate results back from 2 historicals
            List<Sequence<ScanResultValue>> sequences = Lists.newArrayListWithExpectedSize(2);
            sequences.add(factory.createRunner(segment0).run(query, new HashMap<String, Object>()));
            sequences.add(factory.createRunner(segment1).run(query, new HashMap<String, Object>()));
            return new MergeSequence<>(
                query.getResultOrdering(),
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
