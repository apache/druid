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

package org.apache.druid.indexing.kafka.supervisor;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexing.kafka.KafkaIndexTaskClientFactory;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.DruidMonitorSchedulerConfig;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.metadata.TestSupervisorSpec;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertThrows;

public class KafkaSupervisorSpecTest
{
  private final ObjectMapper mapper;

  public KafkaSupervisorSpecTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(TaskStorage.class, null)
            .addValue(TaskMaster.class, null)
            .addValue(IndexerMetadataStorageCoordinator.class, null)
            .addValue(KafkaIndexTaskClientFactory.class, null)
            .addValue(ObjectMapper.class, mapper)
            .addValue(ServiceEmitter.class, new NoopServiceEmitter())
            .addValue(DruidMonitorSchedulerConfig.class, null)
            .addValue(RowIngestionMetersFactory.class, null)
            .addValue(SupervisorStateManagerConfig.class, null)
            .addValue(ExprMacroTable.class.getName(), LookupEnabledTestExprMacroTable.INSTANCE)
    );
    mapper.registerModules((Iterable<Module>) new KafkaIndexTaskModule().getJacksonModules());
  }

  @Test
  public void testSerde() throws IOException
  {
    String json = "{\n"
                  + "  \"type\": \"kafka\",\n"
                  + "  \"dataSchema\": {\n"
                  + "    \"dataSource\": \"metrics-kafka\",\n"
                  + "    \"parser\": {\n"
                  + "      \"type\": \"string\",\n"
                  + "      \"parseSpec\": {\n"
                  + "        \"format\": \"json\",\n"
                  + "        \"timestampSpec\": {\n"
                  + "          \"column\": \"timestamp\",\n"
                  + "          \"format\": \"auto\"\n"
                  + "        },\n"
                  + "        \"dimensionsSpec\": {\n"
                  + "          \"dimensions\": [],\n"
                  + "          \"dimensionExclusions\": [\n"
                  + "            \"timestamp\",\n"
                  + "            \"value\"\n"
                  + "          ]\n"
                  + "        }\n"
                  + "      }\n"
                  + "    },\n"
                  + "    \"metricsSpec\": [\n"
                  + "      {\n"
                  + "        \"name\": \"count\",\n"
                  + "        \"type\": \"count\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_sum\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleSum\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_min\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleMin\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_max\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleMax\"\n"
                  + "      }\n"
                  + "    ],\n"
                  + "    \"granularitySpec\": {\n"
                  + "      \"type\": \"uniform\",\n"
                  + "      \"segmentGranularity\": \"HOUR\",\n"
                  + "      \"queryGranularity\": \"NONE\"\n"
                  + "    }\n"
                  + "  },\n"
                  + "  \"ioConfig\": {\n"
                  + "    \"topic\": \"metrics\",\n"
                  + "    \"consumerProperties\": {\n"
                  + "      \"bootstrap.servers\": \"localhost:9092\"\n"
                  + "    },\n"
                  + "    \"taskCount\": 1\n"
                  + "  }\n"
                  + "}";
    KafkaSupervisorSpec spec = mapper.readValue(json, KafkaSupervisorSpec.class);

    Assert.assertNotNull(spec);
    Assert.assertNotNull(spec.getDataSchema());
    Assert.assertEquals(4, spec.getDataSchema().getAggregators().length);
    Assert.assertNotNull(spec.getIoConfig());
    Assert.assertEquals("metrics", spec.getIoConfig().getTopic());
    Assert.assertNull(spec.getIoConfig().getTopicPattern());
    Assert.assertNotNull(spec.getTuningConfig());
    Assert.assertNull(spec.getContext());
    Assert.assertFalse(spec.isSuspended());
    String serialized = mapper.writeValueAsString(spec);

    // expect default values populated in reserialized string
    Assert.assertTrue(serialized.contains("\"tuningConfig\":{"));
    Assert.assertTrue(serialized.contains("\"indexSpec\":{"));
    Assert.assertTrue(serialized.contains("\"suspended\":false"));
    Assert.assertTrue(serialized.contains("\"parser\":{"));

    KafkaSupervisorSpec spec2 = mapper.readValue(serialized, KafkaSupervisorSpec.class);

    String stable = mapper.writeValueAsString(spec2);

    Assert.assertEquals(serialized, stable);
  }

  @Test
  public void testSerdeWithTopicPattern() throws IOException
  {
    String json = "{\n"
                  + "  \"type\": \"kafka\",\n"
                  + "  \"dataSchema\": {\n"
                  + "    \"dataSource\": \"metrics-kafka\",\n"
                  + "    \"parser\": {\n"
                  + "      \"type\": \"string\",\n"
                  + "      \"parseSpec\": {\n"
                  + "        \"format\": \"json\",\n"
                  + "        \"timestampSpec\": {\n"
                  + "          \"column\": \"timestamp\",\n"
                  + "          \"format\": \"auto\"\n"
                  + "        },\n"
                  + "        \"dimensionsSpec\": {\n"
                  + "          \"dimensions\": [],\n"
                  + "          \"dimensionExclusions\": [\n"
                  + "            \"timestamp\",\n"
                  + "            \"value\"\n"
                  + "          ]\n"
                  + "        }\n"
                  + "      }\n"
                  + "    },\n"
                  + "    \"metricsSpec\": [\n"
                  + "      {\n"
                  + "        \"name\": \"count\",\n"
                  + "        \"type\": \"count\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_sum\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleSum\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_min\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleMin\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_max\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleMax\"\n"
                  + "      }\n"
                  + "    ],\n"
                  + "    \"granularitySpec\": {\n"
                  + "      \"type\": \"uniform\",\n"
                  + "      \"segmentGranularity\": \"HOUR\",\n"
                  + "      \"queryGranularity\": \"NONE\"\n"
                  + "    }\n"
                  + "  },\n"
                  + "  \"ioConfig\": {\n"
                  + "    \"topicPattern\": \"metrics.*\",\n"
                  + "    \"consumerProperties\": {\n"
                  + "      \"bootstrap.servers\": \"localhost:9092\"\n"
                  + "    },\n"
                  + "    \"taskCount\": 1\n"
                  + "  }\n"
                  + "}";
    KafkaSupervisorSpec spec = mapper.readValue(json, KafkaSupervisorSpec.class);

    Assert.assertNotNull(spec);
    Assert.assertNotNull(spec.getDataSchema());
    Assert.assertEquals("metrics.*", spec.getIoConfig().getTopicPattern());
    Assert.assertNull(spec.getIoConfig().getTopic());
    Assert.assertNotNull(spec.getTuningConfig());
    Assert.assertNull(spec.getContext());
    String serialized = mapper.writeValueAsString(spec);

    // expect default values populated in reserialized string
    Assert.assertTrue(serialized.contains("\"topicPattern\":\"metrics.*\""));
    Assert.assertTrue(serialized, serialized.contains("\"topic\":null"));

    KafkaSupervisorSpec spec2 = mapper.readValue(serialized, KafkaSupervisorSpec.class);

    String stable = mapper.writeValueAsString(spec2);

    Assert.assertEquals(serialized, stable);
  }
  @Test
  public void testSerdeWithInputFormat() throws IOException
  {
    String json = "{\n"
                  + "  \"type\": \"kafka\",\n"
                  + "  \"dataSchema\": {\n"
                  + "    \"dataSource\": \"metrics-kafka\",\n"
                  + "    \"timestampSpec\": {\n"
                  + "      \"column\": \"timestamp\",\n"
                  + "      \"format\": \"auto\"\n"
                  + "     },\n"
                  + "    \"dimensionsSpec\": {\n"
                  + "      \"dimensions\": [],\n"
                  + "      \"dimensionExclusions\": [\n"
                  + "        \"timestamp\",\n"
                  + "        \"value\"\n"
                  + "       ]\n"
                  + "    },\n"
                  + "    \"metricsSpec\": [\n"
                  + "      {\n"
                  + "        \"name\": \"count\",\n"
                  + "        \"type\": \"count\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_sum\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleSum\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_min\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleMin\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_max\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleMax\"\n"
                  + "      }\n"
                  + "    ],\n"
                  + "    \"granularitySpec\": {\n"
                  + "      \"type\": \"uniform\",\n"
                  + "      \"segmentGranularity\": \"HOUR\",\n"
                  + "      \"queryGranularity\": \"NONE\"\n"
                  + "    }\n"
                  + "  },\n"
                  + "  \"ioConfig\": {\n"
                  + "    \"topic\": \"metrics\",\n"
                  + "    \"inputFormat\": {\n"
                  + "      \"type\": \"json\",\n"
                  + "      \"flattenSpec\": {\n"
                  + "        \"useFieldDiscovery\": true,\n"
                  + "        \"fields\": []\n"
                  + "      },\n"
                  + "      \"featureSpec\": {}\n"
                  + "    },"
                  + "    \"consumerProperties\": {\n"
                  + "      \"bootstrap.servers\": \"localhost:9092\"\n"
                  + "    },\n"
                  + "    \"taskCount\": 1\n"
                  + "  }\n"
                  + "}";
    KafkaSupervisorSpec spec = mapper.readValue(json, KafkaSupervisorSpec.class);

    Assert.assertNotNull(spec);
    Assert.assertNotNull(spec.getDataSchema());
    Assert.assertEquals(4, spec.getDataSchema().getAggregators().length);
    Assert.assertNotNull(spec.getIoConfig());
    Assert.assertEquals("metrics", spec.getIoConfig().getTopic());
    Assert.assertNull(spec.getIoConfig().getTopicPattern());
    Assert.assertNotNull(spec.getTuningConfig());
    Assert.assertNull(spec.getContext());
    Assert.assertFalse(spec.isSuspended());
    String serialized = mapper.writeValueAsString(spec);

    // expect default values populated in reserialized string
    Assert.assertTrue(serialized.contains("\"tuningConfig\":{"));
    Assert.assertTrue(serialized.contains("\"indexSpec\":{"));
    Assert.assertTrue(serialized.contains("\"suspended\":false"));
    Assert.assertTrue(serialized.contains("\"inputFormat\":{"));

    KafkaSupervisorSpec spec2 = mapper.readValue(serialized, KafkaSupervisorSpec.class);

    String stable = mapper.writeValueAsString(spec2);

    Assert.assertEquals(serialized, stable);
  }

  @Test
  public void testSerdeWithSpec() throws IOException
  {
    String json = "{\n"
                  + "  \"type\": \"kafka\",\n"
                  + "  \"spec\": {\n"
                  + "  \"dataSchema\": {\n"
                  + "    \"dataSource\": \"metrics-kafka\",\n"
                  + "    \"parser\": {\n"
                  + "      \"type\": \"string\",\n"
                  + "      \"parseSpec\": {\n"
                  + "        \"format\": \"json\",\n"
                  + "        \"timestampSpec\": {\n"
                  + "          \"column\": \"timestamp\",\n"
                  + "          \"format\": \"auto\"\n"
                  + "        },\n"
                  + "        \"dimensionsSpec\": {\n"
                  + "          \"dimensions\": [],\n"
                  + "          \"dimensionExclusions\": [\n"
                  + "            \"timestamp\",\n"
                  + "            \"value\"\n"
                  + "          ]\n"
                  + "        }\n"
                  + "      }\n"
                  + "    },\n"
                  + "    \"metricsSpec\": [\n"
                  + "      {\n"
                  + "        \"name\": \"count\",\n"
                  + "        \"type\": \"count\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_sum\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleSum\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_min\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleMin\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_max\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleMax\"\n"
                  + "      }\n"
                  + "    ],\n"
                  + "    \"granularitySpec\": {\n"
                  + "      \"type\": \"uniform\",\n"
                  + "      \"segmentGranularity\": \"HOUR\",\n"
                  + "      \"queryGranularity\": \"NONE\"\n"
                  + "    }\n"
                  + "  },\n"
                  + "  \"ioConfig\": {\n"
                  + "    \"topic\": \"metrics\",\n"
                  + "    \"consumerProperties\": {\n"
                  + "      \"bootstrap.servers\": \"localhost:9092\"\n"
                  + "    },\n"
                  + "    \"taskCount\": 1\n"
                  + "  }\n"
                  + "  }\n"
                  + "}";
    KafkaSupervisorSpec spec = mapper.readValue(json, KafkaSupervisorSpec.class);

    Assert.assertNotNull(spec);
    Assert.assertNotNull(spec.getDataSchema());
    Assert.assertEquals(4, spec.getDataSchema().getAggregators().length);
    Assert.assertNotNull(spec.getIoConfig());
    Assert.assertEquals("metrics", spec.getIoConfig().getTopic());
    Assert.assertNull(spec.getIoConfig().getTopicPattern());
    Assert.assertNotNull(spec.getTuningConfig());
    Assert.assertNull(spec.getContext());
    Assert.assertFalse(spec.isSuspended());
    String serialized = mapper.writeValueAsString(spec);

    // expect default values populated in reserialized string
    Assert.assertTrue(serialized.contains("\"tuningConfig\":{"));
    Assert.assertTrue(serialized.contains("\"indexSpec\":{"));
    Assert.assertTrue(serialized.contains("\"suspended\":false"));
    Assert.assertTrue(serialized.contains("\"parser\":{"));

    KafkaSupervisorSpec spec2 = mapper.readValue(serialized, KafkaSupervisorSpec.class);

    String stable = mapper.writeValueAsString(spec2);

    Assert.assertEquals(serialized, stable);
  }

  @Test
  public void testSerdeWithSpecAndInputFormat() throws IOException
  {
    String json = "{\n"
                  + "  \"type\": \"kafka\",\n"
                  + "  \"spec\": {\n"
                  + "  \"dataSchema\": {\n"
                  + "    \"dataSource\": \"metrics-kafka\",\n"
                  + "    \"timestampSpec\": {\n"
                  + "      \"column\": \"timestamp\",\n"
                  + "      \"format\": \"auto\"\n"
                  + "    },\n"
                  + "    \"dimensionsSpec\": {\n"
                  + "      \"dimensions\": [],\n"
                  + "      \"dimensionExclusions\": [\n"
                  + "        \"timestamp\",\n"
                  + "        \"value\"\n"
                  + "      ]\n"
                  + "    },\n"
                  + "    \"metricsSpec\": [\n"
                  + "      {\n"
                  + "        \"name\": \"count\",\n"
                  + "        \"type\": \"count\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_sum\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleSum\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_min\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleMin\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_max\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleMax\"\n"
                  + "      }\n"
                  + "    ],\n"
                  + "    \"granularitySpec\": {\n"
                  + "      \"type\": \"uniform\",\n"
                  + "      \"segmentGranularity\": \"HOUR\",\n"
                  + "      \"queryGranularity\": \"NONE\"\n"
                  + "    }\n"
                  + "  },\n"
                  + "  \"ioConfig\": {\n"
                  + "    \"topic\": \"metrics\",\n"
                  + "    \"inputFormat\": {\n"
                  + "      \"type\": \"json\",\n"
                  + "      \"flattenSpec\": {\n"
                  + "        \"useFieldDiscovery\": true,\n"
                  + "        \"fields\": []\n"
                  + "      },\n"
                  + "      \"featureSpec\": {}\n"
                  + "    },"
                  + "    \"consumerProperties\": {\n"
                  + "      \"bootstrap.servers\": \"localhost:9092\"\n"
                  + "    },\n"
                  + "    \"taskCount\": 1\n"
                  + "  }\n"
                  + "  }\n"
                  + "}";
    KafkaSupervisorSpec spec = mapper.readValue(json, KafkaSupervisorSpec.class);

    Assert.assertNotNull(spec);
    Assert.assertNotNull(spec.getDataSchema());
    Assert.assertEquals(4, spec.getDataSchema().getAggregators().length);
    Assert.assertNotNull(spec.getIoConfig());
    Assert.assertEquals("metrics", spec.getIoConfig().getTopic());
    Assert.assertNull(spec.getIoConfig().getTopicPattern());
    Assert.assertNotNull(spec.getTuningConfig());
    Assert.assertNull(spec.getContext());
    Assert.assertFalse(spec.isSuspended());
    String serialized = mapper.writeValueAsString(spec);

    // expect default values populated in reserialized string
    Assert.assertTrue(serialized.contains("\"tuningConfig\":{"));
    Assert.assertTrue(serialized.contains("\"indexSpec\":{"));
    Assert.assertTrue(serialized.contains("\"suspended\":false"));
    Assert.assertTrue(serialized.contains("\"inputFormat\":{"));

    KafkaSupervisorSpec spec2 = mapper.readValue(serialized, KafkaSupervisorSpec.class);

    String stable = mapper.writeValueAsString(spec2);

    Assert.assertEquals(serialized, stable);
  }

  @Test
  public void testSuspendResume() throws IOException
  {
    String json = "{\n"
                  + "  \"type\": \"kafka\",\n"
                  + "  \"dataSchema\": {\n"
                  + "    \"dataSource\": \"metrics-kafka\",\n"
                  + "    \"parser\": {\n"
                  + "      \"type\": \"string\",\n"
                  + "      \"parseSpec\": {\n"
                  + "        \"format\": \"json\",\n"
                  + "        \"timestampSpec\": {\n"
                  + "          \"column\": \"timestamp\",\n"
                  + "          \"format\": \"auto\"\n"
                  + "        },\n"
                  + "        \"dimensionsSpec\": {\n"
                  + "          \"dimensions\": [],\n"
                  + "          \"dimensionExclusions\": [\n"
                  + "            \"timestamp\",\n"
                  + "            \"value\"\n"
                  + "          ]\n"
                  + "        }\n"
                  + "      }\n"
                  + "    },\n"
                  + "    \"metricsSpec\": [\n"
                  + "      {\n"
                  + "        \"name\": \"count\",\n"
                  + "        \"type\": \"count\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_sum\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleSum\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_min\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleMin\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_max\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleMax\"\n"
                  + "      }\n"
                  + "    ],\n"
                  + "    \"granularitySpec\": {\n"
                  + "      \"type\": \"uniform\",\n"
                  + "      \"segmentGranularity\": \"HOUR\",\n"
                  + "      \"queryGranularity\": \"NONE\"\n"
                  + "    }\n"
                  + "  },\n"
                  + "  \"ioConfig\": {\n"
                  + "    \"topic\": \"metrics\",\n"
                  + "    \"consumerProperties\": {\n"
                  + "      \"bootstrap.servers\": \"localhost:9092\"\n"
                  + "    },\n"
                  + "    \"taskCount\": 1\n"
                  + "  }\n"
                  + "}";
    KafkaSupervisorSpec spec = mapper.readValue(json, KafkaSupervisorSpec.class);

    Assert.assertNotNull(spec);
    Assert.assertNotNull(spec.getDataSchema());
    Assert.assertEquals(4, spec.getDataSchema().getAggregators().length);
    Assert.assertNotNull(spec.getIoConfig());
    Assert.assertEquals("metrics", spec.getIoConfig().getTopic());
    Assert.assertNull(spec.getIoConfig().getTopicPattern());
    Assert.assertNotNull(spec.getTuningConfig());
    Assert.assertNull(spec.getContext());
    Assert.assertFalse(spec.isSuspended());

    String suspendedSerialized = mapper.writeValueAsString(spec.createSuspendedSpec());

    // expect default values populated in reserialized string
    Assert.assertTrue(suspendedSerialized.contains("\"tuningConfig\":{"));
    Assert.assertTrue(suspendedSerialized.contains("\"indexSpec\":{"));
    Assert.assertTrue(suspendedSerialized.contains("\"suspended\":true"));

    KafkaSupervisorSpec suspendedSpec = mapper.readValue(suspendedSerialized, KafkaSupervisorSpec.class);

    Assert.assertTrue(suspendedSpec.isSuspended());

    String runningSerialized = mapper.writeValueAsString(spec.createRunningSpec());

    KafkaSupervisorSpec runningSpec = mapper.readValue(runningSerialized, KafkaSupervisorSpec.class);

    Assert.assertFalse(runningSpec.isSuspended());
  }

  @Test
  public void test_validateSpecUpdateTo()
  {
    KafkaSupervisorSpec sourceSpec = getSpec("metrics", null);

    // Proposed spec being non-kafka is not allowed
    TestSupervisorSpec otherSpec = new TestSupervisorSpec("test", new Object());
    MatcherAssert.assertThat(
        assertThrows(DruidException.class, () -> sourceSpec.validateSpecUpdateTo(otherSpec)),
        new DruidExceptionMatcher(
            DruidException.Persona.USER,
            DruidException.Category.INVALID_INPUT,
            "invalidInput"
        ).expectMessageIs(
            StringUtils.format("Cannot change spec from type[%s] to type[%s]", sourceSpec.getClass().getSimpleName(), otherSpec.getClass().getSimpleName())
        )
    );

    KafkaSupervisorSpec multiTopicProposedSpec = getSpec(null, "metrics-.*");
    MatcherAssert.assertThat(
        assertThrows(DruidException.class, () -> sourceSpec.validateSpecUpdateTo(multiTopicProposedSpec)),
        new DruidExceptionMatcher(
            DruidException.Persona.USER,
            DruidException.Category.INVALID_INPUT,
            "invalidInput"
        ).expectMessageIs(
             "Update of the input source stream from [(single-topic) metrics] to [(multi-topic) metrics-.*] is not supported for a running supervisor."
             + "\nTo perform the update safely, follow these steps:"
             + "\n(1) Suspend this supervisor, reset its offsets and then terminate it. "
             + "\n(2) Create a new supervisor with the new input source stream."
             + "\nNote that doing the reset can cause data duplication or loss if any topic used in the old supervisor is included in the new one too."
         )
    );

    KafkaSupervisorSpec singleTopicNewStreamProposedSpec = getSpec("metricsNew", null);
    MatcherAssert.assertThat(
        assertThrows(DruidException.class, () -> sourceSpec.validateSpecUpdateTo(singleTopicNewStreamProposedSpec)),
        new DruidExceptionMatcher(
            DruidException.Persona.USER,
            DruidException.Category.INVALID_INPUT,
            "invalidInput"
        ).expectMessageIs(
            "Update of the input source stream from [metrics] to [metricsNew] is not supported for a running supervisor."
            + "\nTo perform the update safely, follow these steps:"
            + "\n(1) Suspend this supervisor, reset its offsets and then terminate it. "
            + "\n(2) Create a new supervisor with the new input source stream."
            + "\nNote that doing the reset can cause data duplication or loss if any topic used in the old supervisor is included in the new one too."
        )
    );

    KafkaSupervisorSpec multiTopicMatchingSourceString = getSpec(null, "metrics");
    MatcherAssert.assertThat(
        assertThrows(DruidException.class, () -> sourceSpec.validateSpecUpdateTo(multiTopicMatchingSourceString)),
        new DruidExceptionMatcher(
            DruidException.Persona.USER,
            DruidException.Category.INVALID_INPUT,
            "invalidInput"
        ).expectMessageIs(
            "Update of the input source stream from [(single-topic) metrics] to [(multi-topic) metrics] is not supported for a running supervisor."
            + "\nTo perform the update safely, follow these steps:"
            + "\n(1) Suspend this supervisor, reset its offsets and then terminate it. "
            + "\n(2) Create a new supervisor with the new input source stream."
            + "\nNote that doing the reset can cause data duplication or loss if any topic used in the old supervisor is included in the new one too."
        )
    );

    // test the inverse as well
    MatcherAssert.assertThat(
        assertThrows(DruidException.class, () -> multiTopicMatchingSourceString.validateSpecUpdateTo(sourceSpec)),
        new DruidExceptionMatcher(
            DruidException.Persona.USER,
            DruidException.Category.INVALID_INPUT,
            "invalidInput"
        ).expectMessageIs(
            "Update of the input source stream from [(multi-topic) metrics] to [(single-topic) metrics] is not supported for a running supervisor."
            + "\nTo perform the update safely, follow these steps:"
            + "\n(1) Suspend this supervisor, reset its offsets and then terminate it. "
            + "\n(2) Create a new supervisor with the new input source stream."
            + "\nNote that doing the reset can cause data duplication or loss if any topic used in the old supervisor is included in the new one too."
        )
    );

    // Test valid spec update. This spec changes context vs the sourceSpec
    KafkaSupervisorSpec validDestSpec = new KafkaSupervisorSpec(
        null,
        null,
        DataSchema.builder().withDataSource("testDs").withAggregators(new CountAggregatorFactory("rows")).withGranularity(new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, null)).build(),
        null,
        new KafkaSupervisorIOConfig(
            "metrics",
            null,
            new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
            null,
            null,
            null,
            Map.of("bootstrap.servers", "localhost:9092"),
            null,
            null,
            null,
            null,
            null,
            true,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            false
        ),
        Map.of(
            "key1",
            "value1",
            "key2",
            "value2"
        ),
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
    sourceSpec.validateSpecUpdateTo(validDestSpec);
  }

  private KafkaSupervisorSpec getSpec(String topic, String topicPattern)
  {
    return new KafkaSupervisorSpec(
      null,
      null,
      DataSchema.builder().withDataSource("testDs").withAggregators(new CountAggregatorFactory("rows")).withGranularity(new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, null)).build(),
      null,
      new KafkaSupervisorIOConfig(
          topic,
          topicPattern,
          new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
          null,
          null,
          null,
          Map.of("bootstrap.servers", "localhost:9092"),
          null,
          null,
          null,
          null,
          null,
          true,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          false
      ),
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
  }
}
