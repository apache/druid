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

package io.druid.query.metadata;

import com.google.common.collect.Lists;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.LegacyDataSource;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.spec.QuerySegmentSpecs;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.TestIndex;
import io.druid.segment.column.ValueType;
import org.junit.Assert;
import org.junit.Test;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class SegmentAnalyzerTest
{
  private static final EnumSet<SegmentMetadataQuery.AnalysisType> emptyAnalyses =
      EnumSet.noneOf(SegmentMetadataQuery.AnalysisType.class);

  @Test
  public void testIncrementalWorks() throws Exception
  {
    testIncrementalWorksHelper(null);
    testIncrementalWorksHelper(emptyAnalyses);
  }

  private void testIncrementalWorksHelper(EnumSet<SegmentMetadataQuery.AnalysisType> analyses) throws Exception
  {
    final List<SegmentAnalysis> results = getSegmentAnalysises(
        new IncrementalIndexSegment(TestIndex.getIncrementalTestIndex(), null),
        analyses
    );

    Assert.assertEquals(1, results.size());

    final SegmentAnalysis analysis = results.get(0);
    Assert.assertEquals(null, analysis.getId());

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
  public void testMappedWorks() throws Exception
  {
    testMappedWorksHelper(null);
    testMappedWorksHelper(emptyAnalyses);
  }

  private void testMappedWorksHelper(EnumSet<SegmentMetadataQuery.AnalysisType> analyses) throws Exception
  {
    final List<SegmentAnalysis> results = getSegmentAnalysises(
        new QueryableIndexSegment("test_1", TestIndex.getMMappedTestIndex()),
        analyses
    );

    Assert.assertEquals(1, results.size());

    final SegmentAnalysis analysis = results.get(0);
    Assert.assertEquals("test_1", analysis.getId());

    final Map<String, ColumnAnalysis> columns = analysis.getColumns();
    Assert.assertEquals(
        TestIndex.COLUMNS.length + 3 - 1,
        columns.size()
    ); // All columns including time and excluding empty/null column

    for (DimensionSchema schema : TestIndex.DIMENSION_SCHEMAS) {
      final String dimension = schema.getName();
      final ColumnAnalysis columnAnalysis = columns.get(dimension);
      if (dimension.equals("null_column")) {
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
        new LegacyDataSource("test"), QuerySegmentSpecs.create("2011/2012"), null, null, null, analyses, false, false
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    return Sequences.toList(query.run(runner, context), Lists.<SegmentAnalysis>newArrayList());
  }
}
