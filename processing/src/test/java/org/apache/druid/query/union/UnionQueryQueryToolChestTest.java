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

package org.apache.druid.query.union;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestTestHelper;
import org.apache.druid.query.ResultSerializationMode;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryQueryToolChest;
import org.apache.druid.query.scan.ScanQueryQueryToolChestTest;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;

public class UnionQueryQueryToolChestTest
{
  @BeforeAll
  public static void setUpClass()
  {
    NullHandling.initializeForTests();
  }

  final UnionQueryLogic queryLogic;
  private ScanQueryQueryToolChest scanToolChest;

  public UnionQueryQueryToolChestTest()
  {
    queryLogic = new UnionQueryLogic();
    scanToolChest = ScanQueryQueryToolChestTest.makeTestScanQueryToolChest();
    DefaultQueryRunnerFactoryConglomerate conglomerate = new DefaultQueryRunnerFactoryConglomerate(
        Collections.emptyMap(),
        ImmutableMap.<Class<? extends Query>, QueryToolChest>builder()
            .put(ScanQuery.class, scanToolChest)
            .build(),
        Collections.emptyMap()
    );
    queryLogic.initialize(conglomerate);
  }

  @Test
  public void testResultArraySignatureWithTimestampResultField()
  {
    RowSignature sig = RowSignature.builder()
        .add("a", ColumnType.STRING)
        .add("b", ColumnType.STRING)
        .build();

    TestScanQuery scan1 = new TestScanQuery("foo", sig)
        .appendRow("a", "a")
        .appendRow("a", "b");
    TestScanQuery scan2 = new TestScanQuery("bar", sig)
        .appendRow("x", "x")
        .appendRow("x", "y");

    List<Query<?>> queries = ImmutableList.of(
        scan1.query,
        scan2.query
    );

    UnionQuery query = new UnionQuery(queries);


    Assert.assertEquals(
        sig,
        query.getResultRowSignature()
    );
  }

  static class TestScanQuery
  {
    final ScanQuery query;
    final List<Object[]> results = new ArrayList<>();

    public TestScanQuery(String sourceName, RowSignature signature)
    {
      this.query = Druids.newScanQueryBuilder()
          .dataSource(sourceName)
          .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2000/3000"))))
          .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
          .columns(signature.getColumnNames())
          .columnTypes(signature.getColumnTypes())
          .build();
    }

    public TestScanQuery appendRow(Object... row)
    {
      results.add(row);
      return this;
    }

    public Sequence<ScanResultValue> makeResultSequence()
    {
      ScanResultValue result = new ScanResultValue(
          QueryRunnerTestHelper.SEGMENT_ID.toString(),
          query.getColumns(),
          convertResultsToListOfLists()
      );
      return Sequences.of(result);
    }

    private List<List<Object>> convertResultsToListOfLists()
    {
      List<List<Object>> resultsRows = new ArrayList<>();
      for (Object[] objects : results) {
        resultsRows.add(Arrays.asList(objects));
      }
      return resultsRows;
    }

    private boolean matchQuery(ScanQuery query)
    {
      return query != null && serializedAsRows(this.query).equals(serializedAsRows(query));
    }

    public Sequence<Object[]> makeResultsAsArrays()
    {
      ScanQueryQueryToolChest scanToolChest = ScanQueryQueryToolChestTest.makeTestScanQueryToolChest();
      return scanToolChest.resultsAsArrays(query, makeResultSequence());
    }
  }

  @Test
  void testQueryRunner()
  {
    RowSignature sig = RowSignature.builder()
        .add("a", ColumnType.STRING)
        .add("b", ColumnType.STRING)
        .build();

    TestScanQuery scan1 = new TestScanQuery("foo", sig)
        .appendRow("a", "a")
        .appendRow("a", "b");
    TestScanQuery scan2 = new TestScanQuery("bar", sig)
        .appendRow("x", "x")
        .appendRow("x", "y");

    UnionQuery query = new UnionQuery(
        ImmutableList.of(
            scan1.query,
            scan2.query
        )
    );
    query = (UnionQuery) serializedAsRows(query);

    QuerySegmentWalker walker = Mockito.mock(QuerySegmentWalker.class);
    Mockito.when(walker.getQueryRunnerForIntervals(argThat(scan1::matchQuery), any()))
        .thenReturn((q, ctx) -> (Sequence) scan1.makeResultSequence());
    Mockito.when(walker.getQueryRunnerForIntervals(argThat(scan2::matchQuery), any()))
        .thenReturn((q, ctx) -> (Sequence) scan2.makeResultSequence());

    QueryRunner<Object> unionRunner = queryLogic.entryPoint(query, walker);
    Sequence results = unionRunner.run(QueryPlus.wrap(query), null);

    QueryToolChestTestHelper.assertArrayResultsEquals(
        Sequences.concat(
            scan1.makeResultsAsArrays(),
            scan2.makeResultsAsArrays()
        ).toList(),
        results
    );
  }

  private static Query<?> serializedAsRows(Query<?> query)
  {
    return query
        .withOverriddenContext(ImmutableMap.of(ResultSerializationMode.CTX_SERIALIZATION_PARAMETER, "rows"));
  }
}
