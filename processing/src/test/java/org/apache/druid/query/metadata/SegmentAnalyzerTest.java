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

import com.google.common.io.CharSource;
import com.google.common.io.Resources;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.NoopAggregator;
import org.apache.druid.query.aggregation.NoopBufferAggregator;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class SegmentAnalyzerTest extends InitializedNullHandlingTest
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
        new TableDataSource("test"), new LegacySegmentSpec("2011/2012"), null, null, null, analyses, false, false
    );
    return runner.run(QueryPlus.wrap(query)).toList();
  }

  static {
    ComplexMetrics.registerSerde(InvalidAggregatorFactory.TYPE, new ComplexMetricSerde()
    {
      @Override
      public String getTypeName()
      {
        return InvalidAggregatorFactory.TYPE;
      }

      @Override
      public ComplexMetricExtractor getExtractor()
      {
        return null;
      }

      @Override
      public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
      {

      }

      @Override
      public ObjectStrategy getObjectStrategy()
      {
        return DummyObjectStrategy.getInstance();
      }
    });
  }

  /**
   * This test verifies that if a segment was created using an unknown/invalid aggregator
   * (which can happen if an aggregator was removed for a later version), then,
   * analyzing the segment doesn't fail and the result of analysis of the complex column
   * is reported as an error.
   * @throws IOException
   */
  @Test
  public void testAnalyzingSegmentWithNonExistentAggregator() throws IOException
  {
    final URL resource = SegmentAnalyzerTest.class.getClassLoader().getResource("druid.sample.numeric.tsv");
    CharSource source = Resources.asByteSource(resource).asCharSource(StandardCharsets.UTF_8);
    String invalid_aggregator = "invalid_aggregator";
    AggregatorFactory[] metrics = new AggregatorFactory[]{
        new DoubleSumAggregatorFactory(TestIndex.DOUBLE_METRICS[0], "index"),
        new HyperUniquesAggregatorFactory("quality_uniques", "quality"),
        new InvalidAggregatorFactory(invalid_aggregator, "quality")
    };
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(DateTimes.of("2011-01-12T00:00:00.000Z").getMillis())
        .withTimestampSpec(new TimestampSpec("ds", "auto", null))
        .withDimensionsSpec(TestIndex.DIMENSIONS_SPEC)
        .withMetrics(metrics)
        .withRollup(true)
        .build();

    final IncrementalIndex retVal = new IncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(10000)
        .buildOnheap();
    IncrementalIndex incrementalIndex = TestIndex.loadIncrementalIndex(retVal, source);
    QueryableIndex queryableIndex = TestIndex.persistRealtimeAndLoadMMapped(incrementalIndex);
    SegmentAnalyzer analyzer = new SegmentAnalyzer(EnumSet.of(SegmentMetadataQuery.AnalysisType.SIZE));
    QueryableIndexSegment segment = new QueryableIndexSegment(
        queryableIndex,
        SegmentId.dummy("ds")
    );
    Map<String, ColumnAnalysis> analyses = analyzer.analyze(segment);
    ColumnAnalysis invalidColumnAnalysis = analyses.get(invalid_aggregator);
    Assert.assertTrue(invalidColumnAnalysis.isError());

    // Run a segment metadata query also to verify it doesn't break
    final List<SegmentAnalysis> results = getSegmentAnalysises(segment, EnumSet.of(SegmentMetadataQuery.AnalysisType.SIZE));
    for (SegmentAnalysis result : results) {
      Assert.assertTrue(result.getColumns().get(invalid_aggregator).isError());
    }
  }

  private static final class DummyObjectStrategy implements ObjectStrategy
  {

    private static final Object TO_RETURN = new Object();
    private static final DummyObjectStrategy INSTANCE = new DummyObjectStrategy();

    private static DummyObjectStrategy getInstance()
    {
      return INSTANCE;
    }

    @Override
    public Class getClazz()
    {
      return Object.class;
    }

    @Nullable
    @Override
    public Object fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      return TO_RETURN;
    }

    @Nullable
    @Override
    public byte[] toBytes(@Nullable Object val)
    {
      return new byte[0];
    }

    @Override
    public int compare(Object o1, Object o2)
    {
      return 0;
    }
  }

  /**
   * Aggregator factory not registered in AggregatorModules and hence not visible in DefaultObjectMapper.
   */
  private static class InvalidAggregatorFactory extends AggregatorFactory
  {
    private final String name;
    private final String fieldName;
    private static final String TYPE = "invalid_complex_column_type";


    public InvalidAggregatorFactory(String name, String fieldName)
    {
      this.name = name;
      this.fieldName = fieldName;
    }

    @Override
    public Aggregator factorize(ColumnSelectorFactory metricFactory)
    {
      return NoopAggregator.instance();
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
    {
      return NoopBufferAggregator.instance();
    }

    @Override
    public Comparator getComparator()
    {
      return null;
    }

    @Nullable
    @Override
    public Object combine(@Nullable Object lhs, @Nullable Object rhs)
    {
      return null;
    }

    @Override
    public AggregatorFactory getCombiningFactory()
    {
      return null;
    }

    @Override
    public List<AggregatorFactory> getRequiredColumns()
    {
      return null;
    }

    @Override
    public Object deserialize(Object object)
    {
      return null;
    }

    @Nullable
    @Override
    public Object finalizeComputation(@Nullable Object object)
    {
      return null;
    }

    @Override
    public String getName()
    {
      return name;
    }

    @Override
    public List<String> requiredFields()
    {
      return Collections.singletonList(fieldName);
    }

    @Override
    public String getTypeName()
    {
      return TYPE;
    }

    @Override
    public int getMaxIntermediateSize()
    {
      return 0;
    }

    @Override
    public byte[] getCacheKey()
    {
      return new byte[0];
    }
  }

}
