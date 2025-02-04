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

package org.apache.druid.quidem;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Injector;
import net.hydromatic.quidem.AbstractCommand;
import net.hydromatic.quidem.Command;
import org.apache.calcite.util.Util;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.stats.DropwizardRowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.IngestionTestBase2;
import org.apache.druid.indexing.common.task.IngestionTestBase2.TestLocalTaskActionClient;
import org.apache.druid.indexing.common.task.IngestionTestBase2.TestLocalTaskActionClientFactory;
import org.apache.druid.indexing.common.task.TestAppenderatorsManager;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.indexing.overlord.TestTaskToolboxFactory;
import org.apache.druid.indexing.test.TestDataSegmentKiller;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.realtime.ChatHandlerProvider;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.timeline.DataSegment;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

public class DruidQuidemIngestCommandHandler extends DruidQuidemCommandHandler
{
  @Override
  public Command parseCommand(List<String> lines, List<String> content, String line)
  {
    if (line.startsWith("ingest")) {
      return new IngestCommand(lines, content);
    }

    return super.parseCommand(lines, content, line);
  }

  static class IngestCommand extends AbstractCommand
  {
    private final List<String> content;
    private final List<String> lines;

    IngestCommand(List<String> lines, List<String> content)
    {
      this.lines = ImmutableList.copyOf(lines);
      this.content = content;
    }

    @Override
    public final String describe(Context context)
    {
      return commandName();
    }

    @Override
    public final void execute(Context context, boolean execute)
    {
      context.echo(content);
      try {
        if (execute) {
          executeIngest(context);
        }
      }
      catch (Exception e) {
        throw new Error(e);
      }
      context.echo(lines);
    }

    private void executeIngest(Context context)
    {
      try {
        Injector injector = DruidConnectionExtras.unwrapOrThrow(context.connection()).getInjector();
        ObjectMapper om = injector.getInstance(ObjectMapper.class);
        SpecificSegmentsQuerySegmentWalker ss = injector.getInstance(SpecificSegmentsQuerySegmentWalker.class);

        String ingestText = Joiner.on("\n").join(content);

        ParallelIndexSupervisorTask ingestTask;
        ingestTask = om.readValue(ingestText, ParallelIndexSupervisorTask.class);

        IngestionTestBase2 itb = new IngestionTestBase2()
        {
        };
        itb.derbyConnectorRule.before();
        itb.setUpIngestionTestBase();

        TestLocalTaskActionClientFactory taskActionClientFactory = itb.new TestLocalTaskActionClientFactory();
        TestTaskToolboxFactory.Builder builder = new TestTaskToolboxFactory.Builder()
            .setChatHandlerProvider(injector.getInstance(ChatHandlerProvider.class))
            .setRowIngestionMetersFactory(new DropwizardRowIngestionMetersFactory())
            .setTaskActionClientFactory(taskActionClientFactory)
            .setAppenderatorsManager(new TestAppenderatorsManager())
            .setSegmentPusher(injector.getInstance(LocalDataSegmentPusher.class))
            .setDataSegmentKiller(new TestDataSegmentKiller())
            // .setConfig(taskConfig)
            // .setIndexIO(new IndexIO(getObjectMapper(), ColumnConfig.DEFAULT))
            // .setTaskActionClientFactory(taskActionClientFactory)
            .setCentralizedTableSchemaConfig(new CentralizedDatasourceSchemaConfig());

        TestTaskToolboxFactory f = builder.build();
        TaskToolbox t = f.build(ingestTask);
        itb.prepareTaskForLocking(ingestTask);
        TaskStatus ts = ingestTask.runTask(t);

        if (true) {

          TestLocalTaskActionClient  taskActionClient = (TestLocalTaskActionClient) t.getTaskActionClient();
          SegmentCacheManagerFactory f1 = itb.segmentCacheManagerFactory;//injector.getInstance(SegmentCacheManagerFactory.class);

//          ss.getSegments();
          SegmentCacheManager sm = f1.manufacturate(new LocalDataSegmentPusherConfig().storageDirectory);
          DataSegment ds = Iterables.getOnlyElement(taskActionClient.getPublishedSegments());
          ReferenceCountingSegment segment = sm.getSegment(ds);

          // ss.add(tsd, tds.newTempFolder());
          ss.add(ds, segment);
        }

        // TaskToolboxFactory aa1 =
        // injector.getInstance(TaskToolboxFactory.class);
        // TaskToolbox toolbox = (TaskToolbox) Proxy
        // .newProxyInstance(getClass().getClassLoader(), new Class<?>[]
        // {TaskToolbox.class}, new Beloved());
        //
        // aa.run(toolbox);
        // spec.getIOConfig().getInputSource();
        // spec.getIOConfig().getInputFormat();
        //
        // TestDataSet tsd= null;

        // make incremental index - possibly
        // incremental index is queriable -

      }
      catch (Exception e) {
        throw new RuntimeException(e);

      }
    }

    static class Beloved implements InvocationHandler
    {

      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
      {
        throw new RuntimeException("Not implemented");
      }

    }

    protected final void executeQuery(Context context, String sql)
    {
      try (
          final Statement statement = context.connection().createStatement();
          final ResultSet resultSet = statement.executeQuery(sql)) {
        // throw away all results
        while (resultSet.next()) {
          Util.discard(false);
        }
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
