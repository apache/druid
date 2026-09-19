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

package org.apache.druid.indexing.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.indexing.DataSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

/**
 * Unit tests for {@link ShareGroupIndexTask} focusing on task type,
 * serialization/deserialization, and basic properties.
 */
public class ShareGroupIndexTaskTest
{
  private ObjectMapper mapper;

  @BeforeEach
  public void setUp()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerSubtypes(
        new NamedType(ShareGroupIndexTask.class, "index_kafka_share_group"),
        new NamedType(ShareGroupIndexTaskIOConfig.class, "kafka_share_group"),
        new NamedType(KafkaIndexTaskTuningConfig.class, "KafkaTuningConfig")
    );
    // Inject the ObjectMapper itself since ShareGroupIndexTask uses @JacksonInject
    mapper.setInjectableValues(new com.fasterxml.jackson.databind.InjectableValues.Std()
        .addValue(ObjectMapper.class, mapper));
  }

  @Test
  public void testTaskType()
  {
    final ShareGroupIndexTask task = createTask("task_1");
    Assertions.assertEquals("index_kafka_share_group", task.getType());
  }

  @Test
  public void testTaskIsAlwaysReady() throws Exception
  {
    final ShareGroupIndexTask task = createTask("task_2");
    Assertions.assertTrue(task.isReady(null));
  }

  @Test
  public void testTaskDataSource()
  {
    final ShareGroupIndexTask task = createTask("task_3");
    Assertions.assertEquals("test_datasource", task.getDataSource());
  }

  @Test
  public void testTaskIdGeneration()
  {
    final ShareGroupIndexTask task = createTask(null);
    Assertions.assertNotNull(task.getId());
    Assertions.assertTrue(task.getId().contains("index_kafka_share_group"));
  }

  @Test
  public void testGracefulStop()
  {
    final ShareGroupIndexTask task = createTask("task_4");
    Assertions.assertFalse(task.isStopRequested());
    task.stopGracefully(null);
    Assertions.assertTrue(task.isStopRequested());
  }

  @Test
  public void testIOConfigAccessor()
  {
    final ShareGroupIndexTask task = createTask("task_5");
    final ShareGroupIndexTaskIOConfig ioConfig = task.getIOConfig();
    Assertions.assertEquals("test-topic", ioConfig.getTopic());
    Assertions.assertEquals("test-share-group", ioConfig.getGroupId());
  }

  @Test
  public void testSerdeRoundTrip() throws IOException
  {
    final ShareGroupIndexTask task = createTask("task_serde");
    final String json = mapper.writeValueAsString(task);

    // Verify the type field is present
    Assertions.assertTrue(json.contains("index_kafka_share_group"));

    final ShareGroupIndexTask deserialized = mapper.readValue(json, ShareGroupIndexTask.class);
    Assertions.assertEquals(task.getId(), deserialized.getId());
    Assertions.assertEquals(task.getDataSource(), deserialized.getDataSource());
    Assertions.assertEquals(task.getType(), deserialized.getType());
    Assertions.assertEquals(task.getIOConfig().getTopic(), deserialized.getIOConfig().getTopic());
    Assertions.assertEquals(task.getIOConfig().getGroupId(), deserialized.getIOConfig().getGroupId());
  }

  @Test
  public void testInputSourceResources()
  {
    final ShareGroupIndexTask task = createTask("task_6");
    Assertions.assertNotNull(task.getInputSourceResources());
    Assertions.assertFalse(task.getInputSourceResources().isEmpty());
  }

  @Test
  public void testDefaultPriorityIsRealtime()
  {
    final ShareGroupIndexTask task = createTask("task_priority_default");
    Assertions.assertEquals(Tasks.DEFAULT_REALTIME_TASK_PRIORITY, task.getPriority());
  }

  @Test
  public void testContextPriorityOverridesDefault()
  {
    final ShareGroupIndexTask task = createTask(
        "task_priority_override",
        ImmutableMap.of(Tasks.PRIORITY_KEY, 99)
    );
    Assertions.assertEquals(99, task.getPriority());
  }

  private ShareGroupIndexTask createTask(String id)
  {
    return createTask(id, null);
  }

  private ShareGroupIndexTask createTask(String id, Map<String, Object> context)
  {
    final DataSchema dataSchema = DataSchema.builder()
        .withDataSource("test_datasource")
        .withTimestamp(new TimestampSpec("__time", null, null))
        .withDimensions(DimensionsSpec.EMPTY)
        .build();

    final Map<String, Object> consumerProps = ImmutableMap.of(
        "bootstrap.servers", "localhost:9092"
    );

    final ShareGroupIndexTaskIOConfig ioConfig = new ShareGroupIndexTaskIOConfig(
        "test-topic",
        "test-share-group",
        consumerProps,
        null,
        null
    );

    final KafkaIndexTaskTuningConfig tuningConfig = new KafkaIndexTaskTuningConfig(
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

    return new ShareGroupIndexTask(
        id,
        null,
        dataSchema,
        tuningConfig,
        ioConfig,
        context,
        mapper
    );
  }
}
