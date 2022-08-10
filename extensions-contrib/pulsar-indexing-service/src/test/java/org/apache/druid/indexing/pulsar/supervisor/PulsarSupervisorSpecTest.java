package org.apache.druid.indexing.pulsar.supervisor;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.indexing.pulsar.PulsarIndexTaskClientFactory;
import org.apache.druid.indexing.pulsar.PulsarIndexingServiceModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class PulsarSupervisorSpecTest
{

  private final ObjectMapper mapper;

  public PulsarSupervisorSpecTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(TaskStorage.class, null)
            .addValue(TaskMaster.class, null)
            .addValue(IndexerMetadataStorageCoordinator.class, null)
            .addValue(PulsarIndexTaskClientFactory.class, null)
            .addValue(ObjectMapper.class, mapper)
            .addValue(ServiceEmitter.class, new NoopServiceEmitter())
            .addValue(DruidMonitorSchedulerConfig.class, null)
            .addValue(RowIngestionMetersFactory.class, null)
            .addValue(SupervisorStateManagerConfig.class, null)
            .addValue(ExprMacroTable.class.getName(), LookupEnabledTestExprMacroTable.INSTANCE)
    );
    mapper.registerModules((Iterable<Module>) new PulsarIndexingServiceModule().getJacksonModules());

  }

  @Test
  public void testSerde() throws IOException
  {
    String json = "{\n"
                  + "  \"type\": \"pulsar\",\n"
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
                  + "    \"topic\": \"test\",\n"
                  + "    \"consumerProperties\": {\n"
                  + "      \"serviceUrl\": \"localhost:6651\"\n"
                  + "    },\n"
                  + "    \"taskCount\": 1\n"
                  + "  }\n"
                  + "}";

    PulsarSupervisorSpec spec = mapper.readValue(json, PulsarSupervisorSpec.class);

    Assert.assertNotNull(spec);
    Assert.assertNotNull(spec.getDataSchema());
    Assert.assertEquals(4, spec.getDataSchema().getAggregators().length);
    Assert.assertNotNull(spec.getIoConfig());
    Assert.assertEquals("test", spec.getIoConfig().getTopic());
    Assert.assertNotNull(spec.getTuningConfig());
    Assert.assertNull(spec.getContext());
    Assert.assertFalse(spec.isSuspended());
    String serialized = mapper.writeValueAsString(spec);

    // expect default values populated in reserialized string
    Assert.assertTrue(serialized.contains("\"tuningConfig\":{"));
    Assert.assertTrue(serialized.contains("\"indexSpec\":{"));
    Assert.assertTrue(serialized.contains("\"suspended\":false"));
    Assert.assertTrue(serialized.contains("\"parser\":{"));

    PulsarSupervisorSpec spec2 = mapper.readValue(serialized, PulsarSupervisorSpec.class);

    String stable = mapper.writeValueAsString(spec2);

    Assert.assertEquals(serialized, stable);

  }

  @Test
  public void testSerdeWithSpec() throws IOException
  {
    String json = "{\n"
                  + "  \"type\": \"pulsar\",\n"
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
                  + "      \"serviceUrl\": \"localhost:9092\"\n"
                  + "    },\n"
                  + "    \"taskCount\": 1\n"
                  + "  }\n"
                  + "  }\n"
                  + "}";
    PulsarSupervisorSpec spec = mapper.readValue(json, PulsarSupervisorSpec.class);

    Assert.assertNotNull(spec);
    Assert.assertNotNull(spec.getDataSchema());
    Assert.assertEquals(4, spec.getDataSchema().getAggregators().length);
    Assert.assertNotNull(spec.getIoConfig());
    Assert.assertEquals("metrics", spec.getIoConfig().getTopic());
    Assert.assertNotNull(spec.getTuningConfig());
    Assert.assertNull(spec.getContext());
    Assert.assertFalse(spec.isSuspended());
    String serialized = mapper.writeValueAsString(spec);

    // expect default values populated in reserialized string
    Assert.assertTrue(serialized.contains("\"tuningConfig\":{"));
    Assert.assertTrue(serialized.contains("\"indexSpec\":{"));
    Assert.assertTrue(serialized.contains("\"suspended\":false"));
    Assert.assertTrue(serialized.contains("\"parser\":{"));

    PulsarSupervisorSpec spec2 = mapper.readValue(serialized, PulsarSupervisorSpec.class);

    String stable = mapper.writeValueAsString(spec2);

    Assert.assertEquals(serialized, stable);
  }

}