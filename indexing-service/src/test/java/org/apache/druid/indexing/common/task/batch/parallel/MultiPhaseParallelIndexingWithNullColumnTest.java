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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputEntityIteratingReader;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@RunWith(Parameterized.class)
public class MultiPhaseParallelIndexingWithNullColumnTest extends AbstractMultiPhaseParallelIndexingTest
{
  private static final TimestampSpec TIMESTAMP_SPEC = new TimestampSpec("ts", "auto", null);
  private static final DimensionsSpec DIMENSIONS_SPEC = new DimensionsSpec(
      DimensionsSpec.getDefaultSchemas(Arrays.asList("ts", "dim1", "dim2"))
  );
  private static final InputFormat JSON_FORMAT = new JsonInputFormat(null, null, null, null, null);
  private static final List<Interval> INTERVAL_TO_INDEX = Collections.singletonList(Intervals.of("2022-01/P1M"));

  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{
            new HashedPartitionsSpec(
                10,
                null,
                ImmutableList.of("ts", "unknownDim")
            )
        },
        new Object[]{
            new DimensionRangePartitionsSpec(
                10,
                null,
                Collections.singletonList("unknownDim"),
                false
            )
        }
    );
  }

  private final PartitionsSpec partitionsSpec;

  public MultiPhaseParallelIndexingWithNullColumnTest(PartitionsSpec partitionsSpec)
  {
    super(LockGranularity.TIME_CHUNK, true, 0., 0.);
    this.partitionsSpec = partitionsSpec;
    getObjectMapper().registerSubtypes(SplittableInlineDataSource.class);
  }

  @Test
  public void testIngestNullColumn() throws JsonProcessingException
  {
    final List<DimensionSchema> dimensionSchemas = DimensionsSpec.getDefaultSchemas(
        Arrays.asList("ts", "unknownDim")
    );
    ParallelIndexSupervisorTask task = new ParallelIndexSupervisorTask(
        null,
        null,
        null,
        new ParallelIndexIngestionSpec(
            new DataSchema(
                DATASOURCE,
                TIMESTAMP_SPEC,
                DIMENSIONS_SPEC.withDimensions(dimensionSchemas),
                DEFAULT_METRICS_SPEC,
                new UniformGranularitySpec(
                    Granularities.DAY,
                    Granularities.MINUTE,
                    INTERVAL_TO_INDEX
                ),
                null
            ),
            new ParallelIndexIOConfig(
                null,
                getInputSource(),
                JSON_FORMAT,
                false,
                null
            ),
            newTuningConfig(
                partitionsSpec,
                2,
                true
            )
        ),
        null
    );

    Assert.assertEquals(TaskState.SUCCESS, getIndexingServiceClient().runAndWait(task).getStatusCode());

    Set<DataSegment> segments = getIndexingServiceClient().getPublishedSegments(task);
    Assert.assertFalse(segments.isEmpty());
    for (DataSegment segment : segments) {
      Assert.assertEquals(dimensionSchemas.size(), segment.getDimensions().size());
      for (int i = 0; i < dimensionSchemas.size(); i++) {
        Assert.assertEquals(dimensionSchemas.get(i).getName(), segment.getDimensions().get(i));
      }
    }
  }

  @Test
  public void testIngestNullColumn_useFieldDiscovery_includeAllDimensions_shouldStoreAllColumns() throws JsonProcessingException
  {
    final List<DimensionSchema> dimensionSchemas = DimensionsSpec.getDefaultSchemas(
        Arrays.asList("ts", "unknownDim", "dim1")
    );
    ParallelIndexSupervisorTask task = new ParallelIndexSupervisorTask(
        null,
        null,
        null,
        new ParallelIndexIngestionSpec(
            new DataSchema(
                DATASOURCE,
                TIMESTAMP_SPEC,
                new DimensionsSpec.Builder().setDimensions(dimensionSchemas).setIncludeAllDimensions(true).build(),
                DEFAULT_METRICS_SPEC,
                new UniformGranularitySpec(
                    Granularities.DAY,
                    Granularities.MINUTE,
                    INTERVAL_TO_INDEX
                ),
                null
            ),
            new ParallelIndexIOConfig(
                null,
                getInputSource(),
                new JsonInputFormat(
                    new JSONPathSpec(true, null),
                    null,
                    null,
                    null,
                    null
                ),
                false,
                null
            ),
            newTuningConfig(
                partitionsSpec,
                2,
                true
            )
        ),
        null
    );

    Assert.assertEquals(TaskState.SUCCESS, getIndexingServiceClient().runAndWait(task).getStatusCode());

    Set<DataSegment> segments = getIndexingServiceClient().getPublishedSegments(task);
    Assert.assertFalse(segments.isEmpty());
    final List<String> expectedExplicitDimensions = ImmutableList.of("ts", "unknownDim", "dim1");
    final Set<String> expectedImplicitDimensions = ImmutableSet.of("dim2", "dim3");
    for (DataSegment segment : segments) {
      Assert.assertEquals(
          expectedExplicitDimensions,
          segment.getDimensions().subList(0, expectedExplicitDimensions.size())
      );
      Assert.assertEquals(
          expectedImplicitDimensions,
          new HashSet<>(segment.getDimensions().subList(expectedExplicitDimensions.size(), segment.getDimensions().size()))
      );
    }
  }

  @Test
  public void testIngestNullColumn_explicitPathSpec_useFieldDiscovery_includeAllDimensions_shouldStoreAllColumns()
      throws JsonProcessingException
  {
    ParallelIndexSupervisorTask task = new ParallelIndexSupervisorTask(
        null,
        null,
        null,
        new ParallelIndexIngestionSpec(
            new DataSchema(
                DATASOURCE,
                TIMESTAMP_SPEC,
                new DimensionsSpec.Builder().setIncludeAllDimensions(true).build(),
                DEFAULT_METRICS_SPEC,
                new UniformGranularitySpec(
                    Granularities.DAY,
                    Granularities.MINUTE,
                    null
                ),
                null
            ),
            new ParallelIndexIOConfig(
                null,
                getInputSource(),
                new JsonInputFormat(
                    new JSONPathSpec(
                        true,
                        ImmutableList.of(
                            new JSONPathFieldSpec(JSONPathFieldType.PATH, "dim1", "$.dim1"),
                            new JSONPathFieldSpec(JSONPathFieldType.PATH, "k", "$.dim4.k")
                        )
                    ),
                    null,
                    null,
                    null,
                    null
                ),
                false,
                null
            ),
            newTuningConfig(
                partitionsSpec,
                2,
                true
            )
        ),
        null
    );

    Assert.assertEquals(TaskState.SUCCESS, getIndexingServiceClient().runAndWait(task).getStatusCode());

    Set<DataSegment> segments = getIndexingServiceClient().getPublishedSegments(task);
    Assert.assertFalse(segments.isEmpty());
    final List<String> expectedExplicitDimensions = ImmutableList.of("dim1", "k");
    final Set<String> expectedImplicitDimensions = ImmutableSet.of("dim2", "dim3");
    for (DataSegment segment : segments) {
      Assert.assertEquals(
          expectedExplicitDimensions,
          segment.getDimensions().subList(0, expectedExplicitDimensions.size())
      );
      Assert.assertEquals(
          expectedImplicitDimensions,
          new HashSet<>(segment.getDimensions().subList(expectedExplicitDimensions.size(), segment.getDimensions().size()))
      );
    }

  }

  @Test
  public void testIngestNullColumn_storeEmptyColumnsOff_shouldNotStoreEmptyColumns() throws JsonProcessingException
  {
    ParallelIndexSupervisorTask task = new ParallelIndexSupervisorTask(
        null,
        null,
        null,
        new ParallelIndexIngestionSpec(
            new DataSchema(
                DATASOURCE,
                TIMESTAMP_SPEC,
                DIMENSIONS_SPEC.withDimensions(
                    DimensionsSpec.getDefaultSchemas(Arrays.asList("ts", "unknownDim"))
                ),
                DEFAULT_METRICS_SPEC,
                new UniformGranularitySpec(
                    Granularities.DAY,
                    Granularities.MINUTE,
                    INTERVAL_TO_INDEX
                ),
                null
            ),
            new ParallelIndexIOConfig(
                null,
                getInputSource(),
                JSON_FORMAT,
                false,
                null
            ),
            newTuningConfig(
                partitionsSpec,
                2,
                true
            )
        ),
        null
    );

    task.addToContext(Tasks.STORE_EMPTY_COLUMNS_KEY, false);
    Assert.assertEquals(TaskState.SUCCESS, getIndexingServiceClient().runAndWait(task).getStatusCode());

    Set<DataSegment> segments = getIndexingServiceClient().getPublishedSegments(task);
    Assert.assertFalse(segments.isEmpty());
    final List<DimensionSchema> expectedDimensions = DimensionsSpec.getDefaultSchemas(
        Collections.singletonList("ts")
    );
    for (DataSegment segment : segments) {
      Assert.assertEquals(expectedDimensions.size(), segment.getDimensions().size());
      for (int i = 0; i < expectedDimensions.size(); i++) {
        Assert.assertEquals(expectedDimensions.get(i).getName(), segment.getDimensions().get(i));
      }
    }
  }

  private InputSource getInputSource() throws JsonProcessingException
  {
    final ObjectMapper mapper = getObjectMapper();
    final List<Map<String, Object>> rows = new ArrayList<>();
    Map<String, Object> row;
    for (int i = 0; i < 3; i++) {
      rows.add(row(StringUtils.format("2022-01-%02d", i + 1), "val1", "val2", null));
    }
    rows.add(row("2022-01-04", null, null, null, ImmutableMap.of("k", "v")));
    final String data = StringUtils.format(
        "%s\n%s\n%s\n%s\n",
        mapper.writeValueAsString(rows.get(0)),
        mapper.writeValueAsString(rows.get(1)),
        mapper.writeValueAsString(rows.get(2)),
        mapper.writeValueAsString(rows.get(3))
    );

    return new SplittableInlineDataSource(ImmutableList.of(data));
  }

  private static Map<String, Object> row(String timestamp, Object... dims)
  {
    Map<String, Object> row = new HashMap<>();
    row.put("ts", timestamp);
    IntStream.range(0, dims.length).forEach(i -> row.put("dim" + (i + 1), dims[i]));
    return row;
  }

  /**
   * Splittable inlineDataSource to run tests with range partitioning which requires the inputSource to be splittable.
   */
  private static final class SplittableInlineDataSource implements SplittableInputSource<String>
  {
    private final List<String> data;

    @JsonCreator
    public SplittableInlineDataSource(@JsonProperty("data") List<String> data)
    {
      this.data = data;
    }

    @JsonProperty
    public List<String> getData()
    {
      return data;
    }

    @Override
    public Stream<InputSplit<String>> createSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
    {
      return data.stream().map(InputSplit::new);
    }

    @Override
    public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
    {
      return data.size();
    }

    @Override
    public InputSource withSplit(InputSplit<String> split)
    {
      return new SplittableInlineDataSource(ImmutableList.of(split.get()));
    }

    @Override
    public boolean needsFormat()
    {
      return true;
    }

    @Override
    public InputSourceReader reader(
        InputRowSchema inputRowSchema,
        @Nullable InputFormat inputFormat,
        File temporaryDirectory
    )
    {
      return new InputEntityIteratingReader(
          inputRowSchema,
          inputFormat,
          data.stream().map(str -> new ByteEntity(StringUtils.toUtf8(str))).iterator(),
          temporaryDirectory
      );
    }
  }
}
