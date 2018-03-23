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
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.client.indexing.NoopIndexingServiceClient;
import io.druid.data.input.impl.CSVParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.indexing.common.TestUtils;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.realtime.firehose.LocalFirehoseFactory;
import io.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import io.druid.server.security.AuthorizerMapper;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SinglePhaseParallelIndexSupervisorTaskSerdeTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final ParseSpec DEFAULT_PARSE_SPEC = new CSVParseSpec(
      new TimestampSpec(
          "ts",
          "auto",
          null
      ),
      new DimensionsSpec(
          DimensionsSpec.getDefaultSchemas(Arrays.asList("ts", "dim")),
          Lists.newArrayList(),
          Lists.newArrayList()
      ),
      null,
      Arrays.asList("ts", "dim", "val"),
      false,
      0
  );

  private final TestUtils testUtils = new TestUtils();

  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper objectMapper = testUtils.getTestObjectMapper();
    objectMapper.registerSubtypes(
        new NamedType(LocalFirehoseFactory.class, "local")
    );

    final SinglePhaseParallelIndexSupervisorTask task = newTask(
        objectMapper,
        Intervals.of("2018/2019")
    );
    final String json = objectMapper.writeValueAsString(task);
    Assert.assertEquals(task, objectMapper.readValue(json, Task.class));
  }

  private SinglePhaseParallelIndexSupervisorTask newTask(
      ObjectMapper objectMapper,
      Interval interval
  )
  {
    // set up ingestion spec
    final SinglePhaseParallelIndexIngestionSpec singlePhaseIngestionSpec = new SinglePhaseParallelIndexIngestionSpec(
        new DataSchema(
            "dataSource",
            objectMapper.convertValue(
                new StringInputRowParser(
                    DEFAULT_PARSE_SPEC,
                    null
                ),
                Map.class
            ),
            new AggregatorFactory[]{
                new LongSumAggregatorFactory("val", "val")
            },
            new UniformGranularitySpec(
                Granularities.DAY,
                Granularities.MINUTE,
                interval == null ? null : Collections.singletonList(interval)
            ),
            null,
            objectMapper
        ),
        new SinglePhaseParallelIndexIOConfig(
            new LocalFirehoseFactory(new File("tmp"), "test_*", null),
            false
        ),
        new SinglePhaseParallelIndexTuningConfig(
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
            2,
            null,
            null,
            null,
            null
        )
    );

    // set up test tools
    return new SinglePhaseParallelIndexSupervisorTask(
        "taskId",
        new TaskResource("group", 1),
        singlePhaseIngestionSpec,
        new HashMap<>(),
        new NoopIndexingServiceClient(),
        new NoopChatHandlerProvider(),
        new AuthorizerMapper(ImmutableMap.of())
    );
  }
}
