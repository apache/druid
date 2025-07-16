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

package org.apache.druid.indexer;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.stats.DropwizardRowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.loading.LocalDataSegmentPuller;
import org.apache.druid.segment.realtime.ChatHandlerProvider;
import org.apache.druid.segment.realtime.NoopChatHandlerProvider;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class HadoopTaskSerdeTest
{
  private final ObjectMapper jsonMapper;
  private final IndexSpec indexSpec = IndexSpec.DEFAULT;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  public HadoopTaskSerdeTest()
  {
    TestUtils testUtils = new TestUtils();
    jsonMapper = testUtils.getTestObjectMapper();
    jsonMapper.registerSubtypes(
        new NamedType(ParallelIndexTuningConfig.class, "index_parallel"),
        new NamedType(IndexTask.IndexTuningConfig.class, "index")
    );
    jsonMapper.registerModules(new HadoopIndexTaskModule().getJacksonModules());
    jsonMapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ExprMacroTable.class, LookupEnabledTestExprMacroTable.INSTANCE)
            .addValue(IndexIO.class, testUtils.getTestIndexIO())
            .addValue(ObjectMapper.class, jsonMapper)
            .addValue(ChatHandlerProvider.class, new NoopChatHandlerProvider())
            .addValue(AuthConfig.class, new AuthConfig())
            .addValue(AuthorizerMapper.class, null)
            .addValue(RowIngestionMetersFactory.class, new DropwizardRowIngestionMetersFactory())
            .addValue(DataSegment.PruneSpecsHolder.class, DataSegment.PruneSpecsHolder.DEFAULT)
            .addValue(OverlordClient.class, TestUtils.OVERLORD_SERVICE_CLIENT)
            .addValue(AuthorizerMapper.class, new AuthorizerMapper(ImmutableMap.of()))
            .addValue(AppenderatorsManager.class, TestUtils.APPENDERATORS_MANAGER)
            .addValue(LocalDataSegmentPuller.class, new LocalDataSegmentPuller())
            .addValue(HadoopTaskConfig.class, new HadoopTaskConfig(null, null))
    );
  }

  @Test
  public void testHadoopIndexTaskSerde() throws Exception
  {
    final HadoopIndexTask task = new HadoopIndexTask(
        null,
        new HadoopIngestionSpec(
            DataSchema.builder()
                      .withDataSource("foo")
                      .withGranularity(
                          new UniformGranularitySpec(
                              Granularities.DAY,
                              null,
                              ImmutableList.of(Intervals.of("2010-01-01/P1D"))
                          )
                      )
                      .withObjectMapper(jsonMapper)
                      .build(),
            new HadoopIOConfig(ImmutableMap.of("paths", "bar"), null, null),
            null
        ),
        null,
        null,
        "blah",
        jsonMapper,
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        new HadoopTaskConfig(null, null)
    );

    final String json = jsonMapper.writeValueAsString(task);

    final HadoopIndexTask task2 = (HadoopIndexTask) jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(
        task.getSpec().getTuningConfig().getJobProperties(),
        task2.getSpec().getTuningConfig().getJobProperties()
    );
    Assert.assertEquals("blah", task.getClasspathPrefix());
    Assert.assertEquals("blah", task2.getClasspathPrefix());
  }

  @Test
  public void testHadoopIndexTaskWithContextSerde() throws Exception
  {
    final HadoopIndexTask task = new HadoopIndexTask(
        null,
        new HadoopIngestionSpec(
            DataSchema.builder()
                      .withDataSource("foo")
                      .withGranularity(
                          new UniformGranularitySpec(
                              Granularities.DAY,
                              null, ImmutableList.of(Intervals.of("2010-01-01/P1D"))
                          )
                      )
                      .withObjectMapper(jsonMapper)
                      .build(),
            new HadoopIOConfig(ImmutableMap.of("paths", "bar"), null, null),
            null
        ),
        null,
        null,
        "blah",
        jsonMapper,
        ImmutableMap.of("userid", 12345, "username", "bob"),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        new HadoopTaskConfig(null, null)
    );

    final String json = jsonMapper.writeValueAsString(task);

    final HadoopIndexTask task2 = (HadoopIndexTask) jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(
        task.getSpec().getTuningConfig().getJobProperties(),
        task2.getSpec().getTuningConfig().getJobProperties()
    );
    Assert.assertEquals("blah", task.getClasspathPrefix());
    Assert.assertEquals("blah", task2.getClasspathPrefix());
    Assert.assertEquals(ImmutableMap.of("userid", 12345, "username", "bob"), task2.getContext());
    Assert.assertEquals(ImmutableMap.of("userid", 12345, "username", "bob"), task2.getSpec().getContext());
  }
}
