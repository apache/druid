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

package io.druid.indexing.materializedview;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.Lists;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.indexer.HadoopTuningConfig;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskStorage;
import io.druid.math.expr.ExprMacroTable;
import io.druid.metadata.MetadataSupervisorManager;
import io.druid.metadata.SQLMetadataSegmentManager;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.expression.LookupEnabledTestExprMacroTable;
import io.druid.segment.TestHelper;
import io.druid.segment.realtime.firehose.ChatHandlerProvider;
import io.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import io.druid.server.security.AuthorizerMapper;
import static org.easymock.EasyMock.createMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class MaterializedViewSupervisorSpecTest 
{
  private ObjectMapper objectMapper = TestHelper.makeJsonMapper();
  
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
            .addValue(SQLMetadataSegmentManager.class, null)
            .addValue(IndexerMetadataStorageCoordinator.class, null)
            .addValue(MaterializedViewTaskConfig.class, new MaterializedViewTaskConfig())
            .addValue(AuthorizerMapper.class, createMock(AuthorizerMapper.class))
            .addValue(ChatHandlerProvider.class, new NoopChatHandlerProvider())
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
        objectMapper,
        null,
        null,
        null,
        null,
        null,
        new MaterializedViewTaskConfig(),
        createMock(AuthorizerMapper.class),
        new NoopChatHandlerProvider()
    );
    MaterializedViewSupervisorSpec spec = objectMapper.readValue(supervisorStr, MaterializedViewSupervisorSpec.class);
    Assert.assertEquals(expected.getBaseDataSource(), spec.getBaseDataSource());
    Assert.assertEquals(expected.getId(), spec.getId());
    Assert.assertEquals(expected.getDataSourceName(), spec.getDataSourceName());
    Assert.assertEquals(expected.getDimensions(), spec.getDimensions());
    Assert.assertEquals(expected.getMetrics(), spec.getMetrics());
  }
}
