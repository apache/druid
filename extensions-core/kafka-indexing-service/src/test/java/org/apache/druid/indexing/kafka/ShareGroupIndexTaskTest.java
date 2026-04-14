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
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.indexing.DataSchema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

/**
 * Unit tests for {@link ShareGroupIndexTask} focusing on task type,
 * serialization/deserialization, and basic properties.
 */
public class ShareGroupIndexTaskTest
{
  private ObjectMapper mapper;

  @Before
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
    Assert.assertEquals("index_kafka_share_group", task.getType());
  }

  @Test
  public void testTaskIsAlwaysReady() throws Exception
  {
    final ShareGroupIndexTask task = createTask("task_2");
    Assert.assertTrue(task.isReady(null));
  }

  @Test
  public void testTaskDataSource()
  {
    final ShareGroupIndexTask task = createTask("task_3");
    Assert.assertEquals("test_datasource", task.getDataSource());
  }

  @Test
  public void testTaskIdGeneration()
  {
    final ShareGroupIndexTask task = createTask(null);
    Assert.assertNotNull(task.getId());
    Assert.assertTrue(task.getId().contains("index_kafka_share_group"));
  }

  @Test
  public void testGracefulStop()
  {
    final ShareGroupIndexTask task = createTask("task_4");
    Assert.assertFalse(task.isStopRequested());
    task.stopGracefully(null);
    Assert.assertTrue(task.isStopRequested());
  }

  @Test
  public void testIOConfigAccessor()
  {
    final ShareGroupIndexTask task = createTask("task_5");
    final ShareGroupIndexTaskIOConfig ioConfig = task.getIOConfig();
    Assert.assertEquals("test-topic", ioConfig.getTopic());
    Assert.assertEquals("test-share-group", ioConfig.getGroupId());
  }

  @Test
  public void testSerdeRoundTrip() throws IOException
  {
    final ShareGroupIndexTask task = createTask("task_serde");
    final String json = mapper.writeValueAsString(task);

    // Verify the type field is present
    Assert.assertTrue(json.contains("index_kafka_share_group"));

    final ShareGroupIndexTask deserialized = mapper.readValue(json, ShareGroupIndexTask.class);
    Assert.assertEquals(task.getId(), deserialized.getId());
    Assert.assertEquals(task.getDataSource(), deserialized.getDataSource());
    Assert.assertEquals(task.getType(), deserialized.getType());
    Assert.assertEquals(task.getIOConfig().getTopic(), deserialized.getIOConfig().getTopic());
    Assert.assertEquals(task.getIOConfig().getGroupId(), deserialized.getIOConfig().getGroupId());
  }

  @Test
  public void testInputSourceResources()
  {
    final ShareGroupIndexTask task = createTask("task_6");
    Assert.assertNotNull(task.getInputSourceResources());
    Assert.assertFalse(task.getInputSourceResources().isEmpty());
  }

  private ShareGroupIndexTask createTask(String id)
  {
    final DataSchema dataSchema = DataSchema.builder()
        .withDataSource("test_datasource")
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
        null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null
    );

    return new ShareGroupIndexTask(
        id,
        null,
        dataSchema,
        tuningConfig,
        ioConfig,
        null,
        mapper
    );
  }
}
