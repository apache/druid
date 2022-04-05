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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HashPartitionMultiPhaseParallelIndexingWithNullColumnTest extends AbstractMultiPhaseParallelIndexingTest
{
  private static final TimestampSpec TIMESTAMP_SPEC = new TimestampSpec("ts", "auto", null);
  private static final DimensionsSpec DIMENSIONS_SPEC = new DimensionsSpec(
      DimensionsSpec.getDefaultSchemas(Arrays.asList("ts", "dim1", "dim2"))
  );
  private static final InputFormat JSON_FORMAT = new JsonInputFormat(null, null, null);
  private static final List<Interval> INTERVAL_TO_INDEX = Collections.singletonList(Intervals.of("2022-01/P1M"));

  public HashPartitionMultiPhaseParallelIndexingWithNullColumnTest()
  {
    super(LockGranularity.TIME_CHUNK, true, 0., 0.);
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
                new HashedPartitionsSpec(
                    10,
                    null,
                    ImmutableList.of("ts", "unknownDim")
                ),
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
      for (int i = 0; i < dimensionSchemas.size(); i++) {
        Assert.assertEquals(dimensionSchemas.get(i).getName(), segment.getDimensions().get(i));
      }
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
                new HashedPartitionsSpec(
                    10,
                    null,
                    ImmutableList.of("ts", "unknownDim")
                ),
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
    for (DataSegment segment : segments) {
      Assert.assertFalse(segment.getDimensions().contains("unknownDim"));
    }
  }

  private InputSource getInputSource() throws JsonProcessingException
  {
    final ObjectMapper mapper = getObjectMapper();
    final List<Map<String, Object>> rows = ImmutableList.of(
        ImmutableMap.of(
            "ts", "2022-01-01",
            "dim1", "val1",
            "dim2", "val11"
        ),
        ImmutableMap.of(
            "ts", "2022-01-02",
            "dim1", "val2",
            "dim2", "val12"
        ),
        ImmutableMap.of(
            "ts", "2022-01-03",
            "dim1", "val3",
            "dim2", "val13"
        )
    );
    final String data = StringUtils.format(
        "%s\n%s\n%s\n",
        mapper.writeValueAsString(rows.get(0)),
        mapper.writeValueAsString(rows.get(1)),
        mapper.writeValueAsString(rows.get(2))
    );
    return new InlineInputSource(data);
  }
}
