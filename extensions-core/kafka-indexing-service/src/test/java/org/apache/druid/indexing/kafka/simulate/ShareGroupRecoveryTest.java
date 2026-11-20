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

package org.apache.druid.indexing.kafka.simulate;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentTransactionalAppendAction;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.KafkaIndexTaskTuningConfig;
import org.apache.druid.indexing.kafka.ShareGroupIndexTask;
import org.apache.druid.indexing.kafka.ShareGroupIndexTaskIOConfig;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class ShareGroupRecoveryTest extends EmbeddedClusterTestBase
{
  private static final String PUBLISH_FAILURE = "Injected share group publication failure";

  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final ShareGroupKafkaResource kafka = new ShareGroupKafkaResource();

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    ShareGroupKafkaResource.assignAvailablePorts(coordinator, overlord, indexer, historical, broker);
    indexer.addProperty("druid.segment.handoff.pollDuration", "PT0.1s");
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .addExtension(KafkaIndexTaskModule.class)
                               .addExtension(PublishFailureModule.class)
                               .addResource(kafka)
                               .useLatchableEmitter()
                               .useDefaultTimeoutForLatchableEmitter(60)
                               .addServer(coordinator)
                               .addServer(overlord)
                               .addServer(indexer)
                               .addServer(historical)
                               .addServer(broker);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testRedeliveryWhenPublicationFailsOrLosesItsResponse(boolean commitBeforeFailure)
  {
    final String topic = dataSource + "_events";
    final String group = dataSource + "_group";
    kafka.createTopicWithPartitions(topic, 1);
    kafka.setShareGroupAutoOffsetReset(group, "earliest");
    final String payload = """
        {"__time":"2025-09-01T00:00:00Z","event_id":"order-1"}
        {"__time":"2025-09-01T00:00:01Z","event_id":"order-2"}
        """;
    kafka.publishRecordsToTopic(topic, List.of(payload.getBytes(StandardCharsets.UTF_8)));

    final ObjectMapper mapper = overlord.bindings().jsonMapper();
    final DataSchema schema = DataSchema.builder()
                                        .withDataSource(dataSource)
                                        .withTimestamp(new TimestampSpec("__time", "auto", null))
                                        .withDimensions(new DimensionsSpec(
                                            DimensionsSpec.getDefaultSchemas(List.of("event_id"))
                                        ))
                                        .withGranularity(
                                            new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, false, null)
                                        )
                                        .build();
    final ShareGroupIndexTaskIOConfig ioConfig = new ShareGroupIndexTaskIOConfig(
        topic,
        group,
        kafka.consumerProperties(),
        new JsonInputFormat(null, null, null, null, true),
        null
    );
    final KafkaIndexTaskTuningConfig tuningConfig = mapper.convertValue(
        Map.of("type", "KafkaTuningConfig"),
        KafkaIndexTaskTuningConfig.class
    );

    final ShareGroupIndexTask first = new PublishFailureTask(
        null,
        null,
        schema,
        tuningConfig,
        ioConfig,
        commitBeforeFailure,
        mapper
    );
    cluster.callApi().submitTask(first);
    final TaskStatus failed = cluster.callApi().waitForTaskToFinish(first.getId(), overlord.latchableEmitter());
    Assertions.assertEquals(TaskState.FAILED, failed.getStatusCode());
    Assertions.assertTrue(failed.getErrorMsg().contains(PUBLISH_FAILURE), failed::getErrorMsg);
    Assertions.assertEquals(
        commitBeforeFailure ? 1 : 0,
        cluster.callApi().getVisibleUsedSegments(dataSource, overlord).size()
    );

    final ShareGroupIndexTask replacement = new ShareGroupIndexTask(
        null,
        null,
        schema,
        tuningConfig,
        ioConfig,
        null,
        mapper
    );
    cluster.callApi().submitTask(replacement);
    indexer.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("ingest/kafka/shareGroup/pollToAck/time")
                      .hasDimension(DruidMetrics.TASK_ID, replacement.getId())
    );
    cluster.callApi().onLeaderOverlord(o -> o.cancelTask(replacement.getId()));
    cluster.callApi().waitForTaskToFinish(replacement.getId(), overlord.latchableEmitter());
    cluster.callApi().waitForAllSegmentsToBeQueryable(dataSource, coordinator, 60_000L);

    final int copies = commitBeforeFailure ? 2 : 1;
    Assertions.assertEquals(
        "order-1," + copies + "\norder-2," + copies,
        cluster.runSql("SELECT event_id, COUNT(*) FROM %s GROUP BY event_id ORDER BY event_id", dataSource)
    );
  }

  public static class PublishFailureModule implements DruidModule
  {
    @Override
    public void configure(Binder binder)
    {
    }

    @Override
    public List<? extends Module> getJacksonModules()
    {
      return List.of(
          new SimpleModule().registerSubtypes(new NamedType(PublishFailureTask.class, PublishFailureTask.TYPE))
      );
    }
  }

  public static class PublishFailureTask extends ShareGroupIndexTask
  {
    private static final String TYPE = "test_kafka_share_publish_failure";
    private final boolean commitBeforeFailure;

    @JsonCreator
    public PublishFailureTask(
        @JsonProperty("id") @Nullable String id,
        @JsonProperty("resource") @Nullable TaskResource taskResource,
        @JsonProperty("dataSchema") DataSchema schema,
        @JsonProperty("tuningConfig") KafkaIndexTaskTuningConfig tuningConfig,
        @JsonProperty("ioConfig") ShareGroupIndexTaskIOConfig ioConfig,
        @JsonProperty("commitBeforeFailure") boolean commitBeforeFailure,
        @JacksonInject ObjectMapper mapper
    )
    {
      super(id, taskResource, schema, tuningConfig, ioConfig, null, mapper);
      this.commitBeforeFailure = commitBeforeFailure;
    }

    @Override
    public String getType()
    {
      return TYPE;
    }

    @JsonProperty
    public boolean isCommitBeforeFailure()
    {
      return commitBeforeFailure;
    }

    @Override
    public TaskStatus runTask(TaskToolbox toolbox) throws Exception
    {
      final TaskActionClient delegate = toolbox.getTaskActionClient();
      final TaskActionClient faultingPublisher = new TaskActionClient()
      {
        @Override
        public <T> T submit(TaskAction<T> action) throws IOException
        {
          if (action instanceof SegmentTransactionalAppendAction append) {
            if (commitBeforeFailure) {
              final SegmentPublishResult result = delegate.submit(append);
              Assertions.assertTrue(result.isSuccess());
            }
            throw new IOException(PUBLISH_FAILURE);
          }
          return delegate.submit(action);
        }
      };
      return super.runTask(new TaskToolbox.Builder(toolbox).taskActionClient(faultingPublisher).build());
    }
  }
}
