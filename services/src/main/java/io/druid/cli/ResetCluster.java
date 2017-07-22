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

package io.druid.cli;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.druid.guice.DruidProcessingModule;
import io.druid.guice.IndexingServiceTaskLogsModule;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.QueryRunnerFactoryModule;
import io.druid.guice.QueryableModule;
import io.druid.guice.annotations.Self;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.task.HadoopTask;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.MetadataStorageConnector;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.segment.loading.DataSegmentKiller;
import io.druid.server.DruidNode;
import io.druid.server.initialization.ServerConfig;
import io.druid.tasklogs.TaskLogKiller;

import java.util.List;

/**
 * Clean-up all Druid state from Metadata and Deep Storage
 */
@Command(
    name = "reset-cluster",
    description = "Cleanup all persisted state from metadata and deep storage."
)
public class ResetCluster extends GuiceRunnable
{
  private static final Logger log = new Logger(ResetCluster.class);

  @Option(name = "--all", description = "delete all state stored in metadata and deep storage")
  private boolean all;

  @Option(name = "--metadataStore", description = "delete all records in metadata storage")
  private boolean metadataStore;

  @Option(name = "--segmentFiles", description = "delete all segment files from deep storage")
  private boolean segmentFiles;

  @Option(name = "--taskLogs", description = "delete all tasklogs")
  private boolean taskLogs;

  @Option(name = "--hadoopWorkingPath", description = "delete hadoopWorkingPath")
  private boolean hadoopWorkingPath;


  public ResetCluster()
  {
    super(log);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.<Module>of(
        // It's unknown if those modules are required in ResetCluster.
        // Maybe some of those modules could be removed.
        // See https://github.com/druid-io/druid/pull/4429#discussion_r123603498
        new DruidProcessingModule(),
        new QueryableModule(),
        new QueryRunnerFactoryModule(),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            JsonConfigProvider.bindInstance(
                binder, Key.get(DruidNode.class, Self.class), new DruidNode("tools", "localhost", -1, null, new ServerConfig())
            );
            JsonConfigProvider.bind(binder, "druid.indexer.task", TaskConfig.class);
          }
        },
        new IndexingServiceTaskLogsModule()
    );
  }

  @Override
  public void run()
  {
    if (all) {
      metadataStore = segmentFiles = taskLogs = hadoopWorkingPath = true;
    }

    final Injector injector = makeInjector();

    if (metadataStore) {
      resetMetadataStore(injector);
    }

    if (segmentFiles) {
      deleteAllSegmentFiles(injector);
    }

    if (taskLogs) {
      deleteAllTaskLogs(injector);
    }

    if (hadoopWorkingPath) {
      deleteIndexerHadoopWorkingDir(injector);
    }
  }

  private void resetMetadataStore(Injector injector)
  {
    log.info("===========================================================================");
    log.info("Deleting all Records from Metadata Storage.");
    log.info("===========================================================================");

    MetadataStorageConnector connector = injector.getInstance(MetadataStorageConnector.class);
    MetadataStorageTablesConfig tablesConfig = injector.getInstance(MetadataStorageTablesConfig.class);

    String[] tables = new String[]{
        tablesConfig.getDataSourceTable(),
        tablesConfig.getPendingSegmentsTable(),
        tablesConfig.getSegmentsTable(),
        tablesConfig.getRulesTable(),
        tablesConfig.getConfigTable(),
        tablesConfig.getTasksTable(),
        tablesConfig.getTaskLockTable(),
        tablesConfig.getTaskLogTable(),
        tablesConfig.getAuditTable(),
        tablesConfig.getSupervisorTable()
    };

    for (String table : tables) {
      connector.deleteAllRecords(table);
    }
  }

  private void deleteAllSegmentFiles(Injector injector)
  {
    try {
      log.info("===========================================================================");
      log.info("Deleting all Segment Files.");
      log.info("===========================================================================");

      DataSegmentKiller segmentKiller = injector.getInstance(DataSegmentKiller.class);
      segmentKiller.killAll();
    }
    catch (Exception ex) {
      log.error(ex, "Failed to cleanup Segment Files.");
    }
  }

  private void deleteAllTaskLogs(Injector injector)
  {
    try {
      log.info("===========================================================================");
      log.info("Deleting all TaskLogs.");
      log.info("===========================================================================");

      TaskLogKiller taskLogKiller = injector.getInstance(TaskLogKiller.class);;
      taskLogKiller.killAll();
    }
    catch (Exception ex) {
      log.error(ex, "Failed to cleanup TaskLogs.");
    }
  }

  private void deleteIndexerHadoopWorkingDir(Injector injector)
  {
    try {
      log.info("===========================================================================");
      log.info("Deleting hadoopWorkingPath.");
      log.info("===========================================================================");

      TaskConfig taskConfig = injector.getInstance(TaskConfig.class);
      HadoopTask.invokeForeignLoader(
          "io.druid.indexer.HadoopWorkingDirCleaner",
          new String[]{
              taskConfig.getHadoopWorkingPath()
          },
          HadoopTask.buildClassLoader(null, taskConfig.getDefaultHadoopCoordinates())
      );
    }
    catch (Exception ex) {
      log.error(ex, "Failed to cleanup indexer hadoop working directory.");
    }
  }
}
