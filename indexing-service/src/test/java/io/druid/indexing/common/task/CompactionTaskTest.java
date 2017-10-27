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

package io.druid.indexing.common.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import io.druid.data.input.impl.DoubleDimensionSchema;
import io.druid.data.input.impl.FloatDimensionSchema;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.LongDimensionSchema;
import io.druid.data.input.impl.NoopInputRowParser;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.guice.GuiceInjectors;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SegmentListUsedAction;
import io.druid.indexing.common.actions.TaskAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.task.IndexTask.IndexIOConfig;
import io.druid.indexing.common.task.IndexTask.IndexIngestionSpec;
import io.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import io.druid.indexing.firehose.IngestSegmentFirehoseFactory;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongMaxAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.first.FloatFirstAggregatorFactory;
import io.druid.query.aggregation.last.DoubleLastAggregatorFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.Metadata;
import io.druid.segment.QueryableIndex;
import io.druid.segment.SimpleQueryableIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.CompressionFactory.LongEncodingStrategy;
import io.druid.segment.data.ListIndexed;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public class CompactionTaskTest
{
  private static final String DATA_SOURCE = "dataSource";
  private static final String TIMESTAMP_COLUMN = "timestamp";
  private static final Interval INTERVAL = Intervals.of("2017-01-01/2018-01-01");
  private static final IndexTuningConfig TUNING_CONFIG = createTuningConfig();

  private static Map<String, DimensionSchema> DIMENSIONS;
  private static Map<String, AggregatorFactory> AGGREGATORS;
  private static Map<DataSegment, File> SEGMENT_MAP;
  private static ObjectMapper objectMapper = new DefaultObjectMapper();
  private static TaskToolbox toolbox;

  @BeforeClass
  public static void setupClass()
  {
    DIMENSIONS = new HashMap<>();
    AGGREGATORS = new HashMap<>();

    DIMENSIONS.put(Column.TIME_COLUMN_NAME, new LongDimensionSchema(Column.TIME_COLUMN_NAME));
    DIMENSIONS.put(TIMESTAMP_COLUMN, new LongDimensionSchema(TIMESTAMP_COLUMN));
    for (int i = 0; i < 5; i++) {
      final StringDimensionSchema schema = new StringDimensionSchema(
          "string_dim_" + i,
          MultiValueHandling.SORTED_ARRAY
      );
      DIMENSIONS.put(schema.getName(), schema);
    }
    for (int i = 0; i < 5; i++) {
      final LongDimensionSchema schema = new LongDimensionSchema("long_dim_" + i);
      DIMENSIONS.put(schema.getName(), schema);
    }
    for (int i = 0; i < 5; i++) {
      final FloatDimensionSchema schema = new FloatDimensionSchema("float_dim_" + i);
      DIMENSIONS.put(schema.getName(), schema);
    }
    for (int i = 0; i < 5; i++) {
      final DoubleDimensionSchema schema = new DoubleDimensionSchema("double_dim_" + i);
      DIMENSIONS.put(schema.getName(), schema);
    }

    AGGREGATORS.put("agg_0", new CountAggregatorFactory("agg_0"));
    AGGREGATORS.put("agg_1", new LongSumAggregatorFactory("agg_1", "long_dim_1"));
    AGGREGATORS.put("agg_2", new LongMaxAggregatorFactory("agg_2", "long_dim_2"));
    AGGREGATORS.put("agg_3", new FloatFirstAggregatorFactory("agg_3", "float_dim_3"));
    AGGREGATORS.put("agg_4", new DoubleLastAggregatorFactory("agg_4", "double_dim_4"));

    SEGMENT_MAP = new HashMap<>(5);
    for (int i = 0; i < 5; i++) {
      SEGMENT_MAP.put(
          new DataSegment(
              DATA_SOURCE,
              INTERVAL,
              "version",
              ImmutableMap.of(),
              findDimensions(i),
              new ArrayList<>(AGGREGATORS.keySet()),
              new NumberedShardSpec(i, 5),
              0,
              1
          ),
          new File("file_" + i)
      );
    }

    toolbox = new TestTaskToolbox(
        new TestTaskActionClient(new ArrayList<>(SEGMENT_MAP.keySet())),
        new TestIndexIO(objectMapper, SEGMENT_MAP),
        SEGMENT_MAP
    );
  }

  private static List<String> findDimensions(int index)
  {
    final List<String> dimensions = new ArrayList<>();
    dimensions.add(TIMESTAMP_COLUMN);
    for (int i = index; i < Math.min(index + 3, 5); i++) {
      dimensions.add("string_dim_" + i);
      dimensions.add("long_dim_" + i);
      dimensions.add("float_dim_" + i);
      dimensions.add("double_dim_" + i);
    }
    return dimensions;
  }

  private static IndexTuningConfig createTuningConfig()
  {
    return new IndexTuningConfig(
        5000000,
        500000,
        1000000,
        null,
        null,
        new IndexSpec(
            new RoaringBitmapSerdeFactory(true),
            CompressionStrategy.LZ4,
            CompressionStrategy.LZF,
            LongEncodingStrategy.LONGS
        ),
        5000,
        true,
        false,
        true,
        false,
        100L
    );
  }

  @Before
  public void setup() throws IOException
  {

  }

  @Test
  public void testCreateIngestionSchema() throws IOException, SegmentLoadingException
  {
    final IndexIngestionSpec ingestionSchema = CompactionTask.createIngestionSchema(
        toolbox,
        DATA_SOURCE,
        INTERVAL,
        TUNING_CONFIG,
        GuiceInjectors.makeStartupInjector(),
        objectMapper
    );

    // assert dataSchema
    final DataSchema dataSchema = ingestionSchema.getDataSchema();
    Assert.assertEquals(DATA_SOURCE, dataSchema.getDataSource());

    final InputRowParser parser = objectMapper.convertValue(dataSchema.getParser(), InputRowParser.class);
    Assert.assertTrue(parser instanceof NoopInputRowParser);
    Assert.assertTrue(parser.getParseSpec() instanceof TimeAndDimsParseSpec);
    Assert.assertEquals(
        new HashSet<>(Sets.difference(
            new HashSet<>(DIMENSIONS.values()), ImmutableSet.of(new LongDimensionSchema(Column.TIME_COLUMN_NAME)))
        ),
        new HashSet<>(parser.getParseSpec().getDimensionsSpec().getDimensions())
    );
    final Set<AggregatorFactory> expectedAggregators = AGGREGATORS.values()
                                                                  .stream()
                                                                  .map(AggregatorFactory::getCombiningFactory)
                                                                  .collect(Collectors.toSet());
    Assert.assertEquals(expectedAggregators, new HashSet<>(Arrays.asList(dataSchema.getAggregators())));
    Assert.assertEquals(
        new ArbitraryGranularitySpec(Granularities.NONE, false, ImmutableList.of(INTERVAL)),
        dataSchema.getGranularitySpec()
    );

    // assert ioConfig
    final IndexIOConfig ioConfig = ingestionSchema.getIOConfig();
    Assert.assertFalse(ioConfig.isAppendToExisting());
    final FirehoseFactory firehoseFactory = ioConfig.getFirehoseFactory();
    Assert.assertTrue(firehoseFactory instanceof IngestSegmentFirehoseFactory);
    final IngestSegmentFirehoseFactory ingestSegmentFirehoseFactory = (IngestSegmentFirehoseFactory) firehoseFactory;
    Assert.assertEquals(DATA_SOURCE, ingestSegmentFirehoseFactory.getDataSource());
    Assert.assertEquals(INTERVAL, ingestSegmentFirehoseFactory.getInterval());
    Assert.assertNull(ingestSegmentFirehoseFactory.getDimensionsFilter());
    // check the order of dimensions
    Assert.assertEquals(
        Lists.newArrayList(
            "timestamp",
            "string_dim_4",
            "long_dim_4",
            "float_dim_4",
            "double_dim_4",
            "string_dim_3",
            "long_dim_3",
            "float_dim_3",
            "double_dim_3",
            "string_dim_2",
            "long_dim_2",
            "float_dim_2",
            "double_dim_2",
            "string_dim_1",
            "long_dim_1",
            "float_dim_1",
            "double_dim_1",
            "string_dim_0",
            "long_dim_0",
            "float_dim_0",
            "double_dim_0"
        ),
        ingestSegmentFirehoseFactory.getDimensions()
    );
    // check the order of metrics
    Assert.assertEquals(
        Lists.newArrayList("agg_4", "agg_3", "agg_2", "agg_1", "agg_0"),
        ingestSegmentFirehoseFactory.getMetrics()
    );

    // assert tuningConfig
    Assert.assertEquals(createTuningConfig(), ingestionSchema.getTuningConfig());
  }

  private static class TestTaskToolbox extends TaskToolbox
  {
    private final Map<DataSegment, File> segmentFileMap;

    TestTaskToolbox(
        TaskActionClient taskActionClient,
        IndexIO indexIO,
        Map<DataSegment, File> segmentFileMap
    )
    {
      super(
          null,
          taskActionClient,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          indexIO,
          null,
          null,
          new IndexMergerV9(objectMapper, indexIO),
          null,
          null,
          null,
          null
      );
      this.segmentFileMap = segmentFileMap;
    }

    @Override
    public Map<DataSegment, File> fetchSegments(List<DataSegment> segments)
        throws SegmentLoadingException
    {
      final Map<DataSegment, File> submap = new HashMap<>(segments.size());
      for (DataSegment segment : segments) {
        final File file = Preconditions.checkNotNull(segmentFileMap.get(segment));
        submap.put(segment, file);
      }
      return submap;
    }
  }

  private static class TestTaskActionClient implements TaskActionClient
  {
    private final List<DataSegment> segments;

    TestTaskActionClient(List<DataSegment> segments)
    {
      this.segments = segments;
    }

    @Override
    public <RetType> RetType submit(TaskAction<RetType> taskAction) throws IOException
    {
      if (!(taskAction instanceof SegmentListUsedAction)) {
        throw new ISE("action[%s] is not supported", taskAction);
      }
      return (RetType) segments;
    }
  }

  private static class TestIndexIO extends IndexIO
  {
    private final Map<File, QueryableIndex> queryableIndexMap;

    TestIndexIO(
        ObjectMapper mapper,
        Map<DataSegment, File> segmentFileMap
    )
    {
      super(mapper, () -> 0);

      queryableIndexMap = new HashMap<>(segmentFileMap.size());
      for (Entry<DataSegment, File> entry : segmentFileMap.entrySet()) {
        final DataSegment segment = entry.getKey();
        final List<String> columnNames = new ArrayList<>(segment.getDimensions().size() + segment.getMetrics().size());
        columnNames.add(Column.TIME_COLUMN_NAME);
        columnNames.addAll(segment.getDimensions());
        columnNames.addAll(segment.getMetrics());
        final Map<String, Column> columnMap = new HashMap<>(columnNames.size());
        final List<AggregatorFactory> aggregatorFactories = new ArrayList<>(segment.getMetrics().size());

        for (String columnName : columnNames) {
          if (DIMENSIONS.containsKey(columnName)) {
            columnMap.put(columnName, createColumn(DIMENSIONS.get(columnName)));
          } else if (AGGREGATORS.containsKey(columnName)) {
            columnMap.put(columnName, createColumn(AGGREGATORS.get(columnName)));
            aggregatorFactories.add(AGGREGATORS.get(columnName));
          }
        }

        final Metadata metadata = new Metadata();
        metadata.setAggregators(aggregatorFactories.toArray(new AggregatorFactory[aggregatorFactories.size()]));
        metadata.setRollup(false);

        queryableIndexMap.put(
            entry.getValue(),
            new SimpleQueryableIndex(
                segment.getInterval(),
                new ListIndexed<>(columnNames, String.class),
                new ListIndexed<>(segment.getDimensions(), String.class),
                null,
                columnMap,
                null,
                metadata
            )
        );
      }
    }

    @Override
    public QueryableIndex loadIndex(File file) throws IOException
    {
      return queryableIndexMap.get(file);
    }
  }

  private static Column createColumn(DimensionSchema dimensionSchema)
  {
    return new ColumnBuilder()
        .setType(IncrementalIndex.TYPE_MAP.get(dimensionSchema.getValueType()))
        .setDictionaryEncodedColumn(() -> null)
        .setBitmapIndex(() -> null)
        .build();
  }

  private static Column createColumn(AggregatorFactory aggregatorFactory)
  {
    return new ColumnBuilder()
        .setType(ValueType.fromString(aggregatorFactory.getTypeName()))
        .build();
  }
}
