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

package org.apache.druid.indexing.common;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.client.indexing.NoopIndexingServiceClient;
import org.apache.druid.data.input.impl.NoopInputFormat;
import org.apache.druid.data.input.impl.NoopInputSource;
import org.apache.druid.guice.FirehoseModule;
import org.apache.druid.indexing.common.stats.DropwizardRowIngestionMetersFactory;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.indexing.common.task.NoopIndexTaskClientFactory;
import org.apache.druid.indexing.common.task.TestAppenderatorsManager;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTaskClient;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.loading.LocalDataSegmentPuller;
import org.apache.druid.segment.loading.LocalLoadSpec;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class TestUtils
{
  public static final IndexingServiceClient INDEXING_SERVICE_CLIENT = new NoopIndexingServiceClient();
  public static final IndexTaskClientFactory<ParallelIndexSupervisorTaskClient> TASK_CLIENT_FACTORY = new NoopIndexTaskClientFactory<>();
  public static final AppenderatorsManager APPENDERATORS_MANAGER = new TestAppenderatorsManager();

  private static final Logger log = new Logger(TestUtils.class);

  private final ObjectMapper jsonMapper;
  private final IndexMergerV9 indexMergerV9;
  private final IndexIO indexIO;
  private final RowIngestionMetersFactory rowIngestionMetersFactory;

  public TestUtils()
  {
    this.jsonMapper = new DefaultObjectMapper();
    indexIO = new IndexIO(
        jsonMapper,
        () -> 0
    );
    indexMergerV9 = new IndexMergerV9(jsonMapper, indexIO, OffHeapMemorySegmentWriteOutMediumFactory.instance());

    this.rowIngestionMetersFactory = new DropwizardRowIngestionMetersFactory();

    jsonMapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ExprMacroTable.class, LookupEnabledTestExprMacroTable.INSTANCE)
            .addValue(IndexIO.class, indexIO)
            .addValue(ObjectMapper.class, jsonMapper)
            .addValue(ChatHandlerProvider.class, new NoopChatHandlerProvider())
            .addValue(AuthConfig.class, new AuthConfig())
            .addValue(AuthorizerMapper.class, null)
            .addValue(RowIngestionMetersFactory.class, rowIngestionMetersFactory)
            .addValue(PruneSpecsHolder.class, PruneSpecsHolder.DEFAULT)
            .addValue(IndexingServiceClient.class, INDEXING_SERVICE_CLIENT)
            .addValue(AuthorizerMapper.class, new AuthorizerMapper(ImmutableMap.of()))
            .addValue(AppenderatorsManager.class, APPENDERATORS_MANAGER)
            .addValue(LocalDataSegmentPuller.class, new LocalDataSegmentPuller())
            .addValue(IndexTaskClientFactory.class, TASK_CLIENT_FACTORY)
    );

    jsonMapper.registerModule(
        new SimpleModule()
        {
          @Override
          public void setupModule(SetupContext context)
          {
            context.registerSubtypes(
                new NamedType(LocalLoadSpec.class, "local"),
                new NamedType(NoopInputSource.class, "noop"),
                new NamedType(NoopInputFormat.class, "noop")
            );
          }
        }
    );

    List<? extends Module> firehoseModules = new FirehoseModule().getJacksonModules();
    firehoseModules.forEach(jsonMapper::registerModule);
  }

  public ObjectMapper getTestObjectMapper()
  {
    return jsonMapper;
  }

  public IndexMergerV9 getTestIndexMergerV9()
  {
    return indexMergerV9;
  }

  public IndexIO getTestIndexIO()
  {
    return indexIO;
  }

  public RowIngestionMetersFactory getRowIngestionMetersFactory()
  {
    return rowIngestionMetersFactory;
  }

  public static boolean conditionValid(IndexingServiceCondition condition)
  {
    return conditionValid(condition, 1000);
  }

  public static boolean conditionValid(IndexingServiceCondition condition, long timeout)
  {
    try {
      Stopwatch stopwatch = Stopwatch.createUnstarted();
      stopwatch.start();
      while (!condition.isValid()) {
        Thread.sleep(100);
        if (stopwatch.elapsed(TimeUnit.MILLISECONDS) > timeout) {
          throw new ISE("Condition[%s] not met", condition);
        }
      }
    }
    catch (Exception e) {
      log.warn(e, "Condition[%s] not met within timeout[%,d]", condition, timeout);
      return false;
    }
    return true;
  }
}
