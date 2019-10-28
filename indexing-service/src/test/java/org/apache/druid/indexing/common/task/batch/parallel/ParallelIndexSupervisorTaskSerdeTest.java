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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.client.indexing.NoopIndexingServiceClient;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.stats.DropwizardRowIngestionMetersFactory;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.TestAppenderatorsManager;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.realtime.firehose.LocalFirehoseFactory;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.server.security.AuthorizerMapper;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ParallelIndexSupervisorTaskSerdeTest
{
  private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

  private static ObjectMapper createObjectMapper()
  {
    TestUtils testUtils = new TestUtils();
    ObjectMapper objectMapper = testUtils.getTestObjectMapper();
    objectMapper.registerSubtypes(
        new NamedType(LocalFirehoseFactory.class, "local")
    );
    return objectMapper;
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void serde() throws IOException
  {
    ParallelIndexSupervisorTask task = new ParallelIndexSupervisorTaskBuilder()
        .ingestionSpec(
            new ParallelIndexIngestionSpecBuilder()
                .inputIntervals(Collections.singletonList(Intervals.of("2018/2019")))
                .build()
        )
        .build();

    String json = OBJECT_MAPPER.writeValueAsString(task);
    Assert.assertEquals(task, OBJECT_MAPPER.readValue(json, Task.class));
  }

  @Test
  public void forceGuaranteedRollupWithMissingIntervals()
  {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage(
        "forceGuaranteedRollup is set but intervals is missing in granularitySpec"
    );

    Integer numShards = 2;
    new ParallelIndexSupervisorTaskBuilder()
        .ingestionSpec(
            new ParallelIndexIngestionSpecBuilder()
                .forceGuaranteedRollup(true)
                .partitionsSpec(new HashedPartitionsSpec(null, numShards, null))
                .build()
        )
        .build();
  }

  @Test
  public void forceGuaranteedRollupWithMissingNumShards()
  {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage(
        "forceGuaranteedRollup is set but numShards is missing in partitionsSpec"
    );

    Integer numShards = null;
    new ParallelIndexSupervisorTaskBuilder()
        .ingestionSpec(
            new ParallelIndexIngestionSpecBuilder()
                .forceGuaranteedRollup(true)
                .partitionsSpec(new HashedPartitionsSpec(null, numShards, null))
                .build()
        )
        .build();
  }

  private static class ParallelIndexSupervisorTaskBuilder
  {
    private static final String ID = "taskId";
    private final TaskResource taskResource = new TaskResource("group", 1);
    private final Map<String, Object> context = Collections.emptyMap();
    private final IndexingServiceClient indexingServiceClient = new NoopIndexingServiceClient();
    private final ChatHandlerProvider chatHandlerProvider = new NoopChatHandlerProvider();
    private final AuthorizerMapper authorizerMapper = new AuthorizerMapper(Collections.emptyMap());
    private final RowIngestionMetersFactory rowIngestionMetersFactory = new DropwizardRowIngestionMetersFactory();
    private final AppenderatorsManager appenderatorsManager = new TestAppenderatorsManager();

    private ParallelIndexIngestionSpec ingestionSpec;

    ParallelIndexSupervisorTaskBuilder ingestionSpec(ParallelIndexIngestionSpec ingestionSpec)
    {
      this.ingestionSpec = ingestionSpec;
      return this;
    }

    ParallelIndexSupervisorTask build()
    {
      return new ParallelIndexSupervisorTask(
          ID,
          null,
          taskResource,
          ingestionSpec,
          context,
          indexingServiceClient,
          chatHandlerProvider,
          authorizerMapper,
          rowIngestionMetersFactory,
          appenderatorsManager
      );
    }
  }

  private static class ParallelIndexIngestionSpecBuilder
  {
    private static final ParseSpec PARSE_SPEC = new CSVParseSpec(
        new TimestampSpec(
            "ts",
            "auto",
            null
        ),
        new DimensionsSpec(
            DimensionsSpec.getDefaultSchemas(Arrays.asList("ts", "dim")),
            new ArrayList<>(),
            new ArrayList<>()
        ),
        null,
        Arrays.asList("ts", "dim", "val"),
        false,
        0
    );

    private static final TypeReference<Map<String, Object>> PARSER_TYPE = new TypeReference<Map<String, Object>>()
    {
    };

    private final ParallelIndexIOConfig ioConfig = new ParallelIndexIOConfig(
        new LocalFirehoseFactory(new File("tmp"), "test_*", null),
        false
    );

    // For dataSchema.granularitySpec
    @Nullable
    private List<Interval> inputIntervals = null;

    // For tuningConfig
    @Nullable
    private Boolean forceGuaranteedRollup = null;
    @Nullable
    PartitionsSpec partitionsSpec = null;

    ParallelIndexIngestionSpecBuilder inputIntervals(List<Interval> inputIntervals)
    {
      this.inputIntervals = inputIntervals;
      return this;
    }

    @SuppressWarnings("SameParameterValue")
    ParallelIndexIngestionSpecBuilder forceGuaranteedRollup(boolean forceGuaranteedRollup)
    {
      this.forceGuaranteedRollup = forceGuaranteedRollup;
      return this;
    }

    ParallelIndexIngestionSpecBuilder partitionsSpec(PartitionsSpec partitionsSpec)
    {
      this.partitionsSpec = partitionsSpec;
      return this;
    }

    ParallelIndexIngestionSpec build()
    {
      DataSchema dataSchema = new DataSchema(
          "dataSource",
          OBJECT_MAPPER.convertValue(
              new StringInputRowParser(PARSE_SPEC, null),
              PARSER_TYPE
          ),
          new AggregatorFactory[]{
              new LongSumAggregatorFactory("val", "val")
          },
          new UniformGranularitySpec(Granularities.DAY, Granularities.MINUTE, inputIntervals),
          null,
          OBJECT_MAPPER
      );

      ParallelIndexTuningConfig tuningConfig = new ParallelIndexTuningConfig(
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          partitionsSpec,
          null,
          null,
          null,
          forceGuaranteedRollup,
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
          null
      );

      return new ParallelIndexIngestionSpec(dataSchema, ioConfig, tuningConfig);
    }
  }
}
