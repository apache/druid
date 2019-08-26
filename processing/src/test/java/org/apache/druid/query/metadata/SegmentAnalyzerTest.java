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

package org.apache.druid.query.metadata;

import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.query.LegacyDataSource;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Test;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

/**
 */
public class SegmentAnalyzerTest
{
  private static final EnumSet<SegmentMetadataQuery.AnalysisType> EMPTY_ANALYSES =
      EnumSet.noneOf(SegmentMetadataQuery.AnalysisType.class);

  @Test
  public void testIncrementalWorks()
  {
    testIncrementalWorksHelper(null);
    testIncrementalWorksHelper(EMPTY_ANALYSES);
  }

  private void testIncrementalWorksHelper(EnumSet<SegmentMetadataQuery.AnalysisType> analyses)
  {
    final List<SegmentAnalysis> results = getSegmentAnalysises(
        new IncrementalIndexSegment(TestIndex.getIncrementalTestIndex(), SegmentId.dummy("ds")),
        analyses
    );

    Assert.assertEquals(1, results.size());

    final SegmentAnalysis analysis = results.get(0);
    Assert.assertEquals(SegmentId.dummy("ds").toString(), analysis.getId());

    final Map<String, ColumnAnalysis> columns = analysis.getColumns();

    Assert.assertEquals(
        TestIndex.COLUMNS.length + 3,
        columns.size()
    ); // All columns including time and empty/null column

    for (DimensionSchema schema : TestIndex.DIMENSION_SCHEMAS) {
      final String dimension = schema.getName();
      final ColumnAnalysis columnAnalysis = columns.get(dimension);
      final boolean isString = schema.getValueType().name().equals(ValueType.STRING.name());

      Assert.assertEquals(dimension, schema.getValueType().name(), columnAnalysis.getType());
      Assert.assertEquals(dimension, 0, columnAnalysis.getSize());
      if (isString) {
        if (analyses == null) {
          Assert.assertTrue(dimension, columnAnalysis.getCardinality() > 0);
        } else {
          Assert.assertEquals(dimension, 0, columnAnalysis.getCardinality().longValue());
        }
      } else {
        Assert.assertNull(dimension, columnAnalysis.getCardinality());
      }
    }

    for (String metric : TestIndex.DOUBLE_METRICS) {
      final ColumnAnalysis columnAnalysis = columns.get(metric);
      Assert.assertEquals(metric, ValueType.DOUBLE.name(), columnAnalysis.getType());
      Assert.assertEquals(metric, 0, columnAnalysis.getSize());
      Assert.assertNull(metric, columnAnalysis.getCardinality());
    }

    for (String metric : TestIndex.FLOAT_METRICS) {
      final ColumnAnalysis columnAnalysis = columns.get(metric);
      Assert.assertEquals(metric, ValueType.FLOAT.name(), columnAnalysis.getType());
      Assert.assertEquals(metric, 0, columnAnalysis.getSize());
      Assert.assertNull(metric, columnAnalysis.getCardinality());
    }
  }

  @Test
  public void testMappedWorks()
  {
    testMappedWorksHelper(null);
    testMappedWorksHelper(EMPTY_ANALYSES);
  }

  private void testMappedWorksHelper(EnumSet<SegmentMetadataQuery.AnalysisType> analyses)
  {
    final List<SegmentAnalysis> results = getSegmentAnalysises(
        new QueryableIndexSegment(TestIndex.getMMappedTestIndex(), SegmentId.dummy("test_1")),
        analyses
    );

    Assert.assertEquals(1, results.size());

    final SegmentAnalysis analysis = results.get(0);
    Assert.assertEquals(SegmentId.dummy("test_1").toString(), analysis.getId());

    final Map<String, ColumnAnalysis> columns = analysis.getColumns();
    Assert.assertEquals(
        TestIndex.COLUMNS.length + 3 - 1,
        columns.size()
    ); // All columns including time and excluding empty/null column

    for (DimensionSchema schema : TestIndex.DIMENSION_SCHEMAS) {
      final String dimension = schema.getName();
      final ColumnAnalysis columnAnalysis = columns.get(dimension);
      if ("null_column".equals(dimension)) {
        Assert.assertNull(columnAnalysis);
      } else {
        final boolean isString = schema.getValueType().name().equals(ValueType.STRING.name());
        Assert.assertEquals(dimension, schema.getValueType().name(), columnAnalysis.getType());
        Assert.assertEquals(dimension, 0, columnAnalysis.getSize());

        if (isString) {
          if (analyses == null) {
            Assert.assertTrue(dimension, columnAnalysis.getCardinality() > 0);
          } else {
            Assert.assertEquals(dimension, 0, columnAnalysis.getCardinality().longValue());
          }
        } else {
          Assert.assertNull(dimension, columnAnalysis.getCardinality());
        }
      }
    }

    for (String metric : TestIndex.DOUBLE_METRICS) {
      final ColumnAnalysis columnAnalysis = columns.get(metric);

      Assert.assertEquals(metric, ValueType.DOUBLE.name(), columnAnalysis.getType());
      Assert.assertEquals(metric, 0, columnAnalysis.getSize());
      Assert.assertNull(metric, columnAnalysis.getCardinality());
    }

    for (String metric : TestIndex.FLOAT_METRICS) {
      final ColumnAnalysis columnAnalysis = columns.get(metric);
      Assert.assertEquals(metric, ValueType.FLOAT.name(), columnAnalysis.getType());
      Assert.assertEquals(metric, 0, columnAnalysis.getSize());
      Assert.assertNull(metric, columnAnalysis.getCardinality());
    }
  }

  /**
   * *Awesome* method name auto-generated by IntelliJ!  I love IntelliJ!
   *
   * @param index
   *
   * @return
   */
  private List<SegmentAnalysis> getSegmentAnalysises(Segment index, EnumSet<SegmentMetadataQuery.AnalysisType> analyses)
  {
    final QueryRunner runner = QueryRunnerTestHelper.makeQueryRunner(
        (QueryRunnerFactory) new SegmentMetadataQueryRunnerFactory(
            new SegmentMetadataQueryQueryToolChest(new SegmentMetadataQueryConfig()),
            QueryRunnerTestHelper.NOOP_QUERYWATCHER
        ),
        index,
        null
    );

    final SegmentMetadataQuery query = new SegmentMetadataQuery(
        new LegacyDataSource("test"), new LegacySegmentSpec("2011/2012"), null, null, null, analyses, false, false
    );
    return runner.run(QueryPlus.wrap(query)).toList();
  }
}
