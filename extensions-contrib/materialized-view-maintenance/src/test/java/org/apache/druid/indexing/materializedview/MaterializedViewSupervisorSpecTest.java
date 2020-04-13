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

package org.apache.druid.indexing.materializedview;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.indexer.HadoopTuningConfig;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.metadata.SqlSegmentsMetadataManager;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.server.security.AuthorizerMapper;
import org.easymock.EasyMock;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

public class MaterializedViewSupervisorSpecTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final ObjectMapper objectMapper = TestHelper.makeJsonMapper();

  @Before
  public void setup()
  {
    objectMapper.registerSubtypes(new NamedType(MaterializedViewSupervisorSpec.class, "derivativeDataSource"));
    objectMapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(TaskMaster.class, null)
            .addValue(TaskStorage.class, null)
            .addValue(ExprMacroTable.class.getName(), LookupEnabledTestExprMacroTable.INSTANCE)
            .addValue(ObjectMapper.class, objectMapper)
            .addValue(MetadataSupervisorManager.class, null)
            .addValue(SqlSegmentsMetadataManager.class, null)
            .addValue(IndexerMetadataStorageCoordinator.class, null)
            .addValue(MaterializedViewTaskConfig.class, new MaterializedViewTaskConfig())
            .addValue(AuthorizerMapper.class, EasyMock.createMock(AuthorizerMapper.class))
            .addValue(ChatHandlerProvider.class, new NoopChatHandlerProvider())
            .addValue(SupervisorStateManagerConfig.class, new SupervisorStateManagerConfig())
    );
  }

  @Test
  public void testSupervisorSerialization() throws IOException
  {
    String supervisorStr = "{\n" +
                           "  \"type\" : \"derivativeDataSource\",\n" +
                           "  \"baseDataSource\": \"wikiticker\",\n" +
                           "  \"dimensionsSpec\":{\n" +
                           "            \"dimensions\" : [\n" +
                           "              \"isUnpatrolled\",\n" +
                           "              \"metroCode\",\n" +
                           "              \"namespace\",\n" +
                           "              \"page\",\n" +
                           "              \"regionIsoCode\",\n" +
                           "              \"regionName\",\n" +
                           "              \"user\"\n" +
                           "            ]\n" +
                           "          },\n" +
                           "    \"metricsSpec\" : [\n" +
                           "        {\n" +
                           "          \"name\" : \"count\",\n" +
                           "          \"type\" : \"count\"\n" +
                           "        },\n" +
                           "        {\n" +
                           "          \"name\" : \"added\",\n" +
                           "          \"type\" : \"longSum\",\n" +
                           "          \"fieldName\" : \"added\"\n" +
                           "        }\n" +
                           "      ],\n" +
                           "  \"tuningConfig\": {\n" +
                           "      \"type\" : \"hadoop\"\n" +
                           "  }\n" +
                           "}";
    MaterializedViewSupervisorSpec expected = new MaterializedViewSupervisorSpec(
        "wikiticker",
        new DimensionsSpec(
            Lists.newArrayList(
                new StringDimensionSchema("isUnpatrolled"),
                new StringDimensionSchema("metroCode"),
                new StringDimensionSchema("namespace"),
                new StringDimensionSchema("page"),
                new StringDimensionSchema("regionIsoCode"),
                new StringDimensionSchema("regionName"),
                new StringDimensionSchema("user")
            ),
            null,
            null
        ),
        new AggregatorFactory[]{
            new CountAggregatorFactory("count"),
            new LongSumAggregatorFactory("added", "added")
        },
        HadoopTuningConfig.makeDefaultTuningConfig(),
        null,
        null,
        null,
        null,
        null,
        false,
        objectMapper,
        null,
        null,
        null,
        null,
        null,
        new MaterializedViewTaskConfig(),
        EasyMock.createMock(AuthorizerMapper.class),
        new NoopChatHandlerProvider(),
        new SupervisorStateManagerConfig()
    );
    MaterializedViewSupervisorSpec spec = objectMapper.readValue(supervisorStr, MaterializedViewSupervisorSpec.class);
    Assert.assertEquals(expected.getBaseDataSource(), spec.getBaseDataSource());
    Assert.assertEquals(expected.getId(), spec.getId());
    Assert.assertEquals(expected.getDataSourceName(), spec.getDataSourceName());
    Assert.assertEquals(expected.getDimensions(), spec.getDimensions());
    Assert.assertEquals(expected.getMetrics(), spec.getMetrics());
  }

  @Test
  public void testSuspendResuume() throws IOException
  {
    String supervisorStr = "{\n" +
                           "  \"type\" : \"derivativeDataSource\",\n" +
                           "  \"baseDataSource\": \"wikiticker\",\n" +
                           "  \"dimensionsSpec\":{\n" +
                           "            \"dimensions\" : [\n" +
                           "              \"isUnpatrolled\",\n" +
                           "              \"metroCode\",\n" +
                           "              \"namespace\",\n" +
                           "              \"page\",\n" +
                           "              \"regionIsoCode\",\n" +
                           "              \"regionName\",\n" +
                           "              \"user\"\n" +
                           "            ]\n" +
                           "          },\n" +
                           "    \"metricsSpec\" : [\n" +
                           "        {\n" +
                           "          \"name\" : \"count\",\n" +
                           "          \"type\" : \"count\"\n" +
                           "        },\n" +
                           "        {\n" +
                           "          \"name\" : \"added\",\n" +
                           "          \"type\" : \"longSum\",\n" +
                           "          \"fieldName\" : \"added\"\n" +
                           "        }\n" +
                           "      ],\n" +
                           "  \"tuningConfig\": {\n" +
                           "      \"type\" : \"hadoop\"\n" +
                           "  }\n" +
                           "}";

    MaterializedViewSupervisorSpec spec = objectMapper.readValue(supervisorStr, MaterializedViewSupervisorSpec.class);
    Assert.assertFalse(spec.isSuspended());

    String suspendedSerialized = objectMapper.writeValueAsString(spec.createSuspendedSpec());
    MaterializedViewSupervisorSpec suspendedSpec = objectMapper.readValue(
        suspendedSerialized,
        MaterializedViewSupervisorSpec.class
    );
    Assert.assertTrue(suspendedSpec.isSuspended());

    String runningSerialized = objectMapper.writeValueAsString(spec.createRunningSpec());
    MaterializedViewSupervisorSpec runningSpec = objectMapper.readValue(
        runningSerialized,
        MaterializedViewSupervisorSpec.class
    );
    Assert.assertFalse(runningSpec.isSuspended());
  }

  @Test
  public void testEmptyBaseDataSource()
  {
    expectedException.expect(CoreMatchers.instanceOf(IllegalArgumentException.class));
    expectedException.expectMessage(
        "baseDataSource cannot be null or empty. Please provide a baseDataSource."
    );
    //noinspection ResultOfObjectAllocationIgnored (this method call will trigger the expected exception)
    new MaterializedViewSupervisorSpec(
        "",
        new DimensionsSpec(
            Lists.newArrayList(
                new StringDimensionSchema("isUnpatrolled"),
                new StringDimensionSchema("metroCode"),
                new StringDimensionSchema("namespace"),
                new StringDimensionSchema("page"),
                new StringDimensionSchema("regionIsoCode"),
                new StringDimensionSchema("regionName"),
                new StringDimensionSchema("user")
            ),
            null,
            null
        ),
        new AggregatorFactory[]{
            new CountAggregatorFactory("count"),
            new LongSumAggregatorFactory("added", "added")
        },
        HadoopTuningConfig.makeDefaultTuningConfig(),
        null,
        null,
        null,
        null,
        null,
        false,
        objectMapper,
        null,
        null,
        null,
        null,
        null,
        new MaterializedViewTaskConfig(),
        EasyMock.createMock(AuthorizerMapper.class),
        new NoopChatHandlerProvider(),
        new SupervisorStateManagerConfig()
    );
  }

  @Test
  public void testNullBaseDataSource()
  {
    expectedException.expect(CoreMatchers.instanceOf(IllegalArgumentException.class));
    expectedException.expectMessage(
        "baseDataSource cannot be null or empty. Please provide a baseDataSource."
    );
    //noinspection ResultOfObjectAllocationIgnored (this method call will trigger the expected exception)
    new MaterializedViewSupervisorSpec(
        null,
        new DimensionsSpec(
            Lists.newArrayList(
                new StringDimensionSchema("isUnpatrolled"),
                new StringDimensionSchema("metroCode"),
                new StringDimensionSchema("namespace"),
                new StringDimensionSchema("page"),
                new StringDimensionSchema("regionIsoCode"),
                new StringDimensionSchema("regionName"),
                new StringDimensionSchema("user")
            ),
            null,
            null
        ),
        new AggregatorFactory[]{
            new CountAggregatorFactory("count"),
            new LongSumAggregatorFactory("added", "added")
        },
        HadoopTuningConfig.makeDefaultTuningConfig(),
        null,
        null,
        null,
        null,
        null,
        false,
        objectMapper,
        null,
        null,
        null,
        null,
        null,
        new MaterializedViewTaskConfig(),
        EasyMock.createMock(AuthorizerMapper.class),
        new NoopChatHandlerProvider(),
        new SupervisorStateManagerConfig()
    );
  }
}
