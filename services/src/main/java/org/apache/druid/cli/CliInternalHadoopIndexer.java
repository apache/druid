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

package org.apache.druid.cli;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.indexer.HadoopDruidDetermineConfigurationJob;
import org.apache.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.druid.indexer.HadoopDruidIndexerJob;
import org.apache.druid.indexer.HadoopIngestionSpec;
import org.apache.druid.indexer.JobHelper;
import org.apache.druid.indexer.Jobby;
import org.apache.druid.indexer.MetadataStorageUpdaterJobHandler;
import org.apache.druid.indexer.path.MetadataStoreBasedUsedSegmentLister;
import org.apache.druid.indexer.updater.MetadataStorageUpdaterJobSpec;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.MetadataStorageTablesConfig;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 */
@Command(
    name = "hadoop-indexer",
    description = "Runs the batch Hadoop Druid Indexer, see https://druid.apache.org/docs/latest/Batch-ingestion.html for a description."
)
public class CliInternalHadoopIndexer extends GuiceRunnable
{
  private static final Logger log = new Logger(CliHadoopIndexer.class);

  @Arguments(description = "A JSON object or the path to a file that contains a JSON object", required = true)
  private String argumentSpec;

  private HadoopDruidIndexerConfig config;

  public CliInternalHadoopIndexer()
  {
    super(log);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        binder -> {
          binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/internal-hadoop-indexer");
          binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
          binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);

          // bind metadata storage config based on HadoopIOConfig
          MetadataStorageUpdaterJobSpec metadataSpec = getHadoopDruidIndexerConfig().getSchema()
                                                                                    .getIOConfig()
                                                                                    .getMetadataUpdateSpec();

          binder.bind(new TypeLiteral<Supplier<MetadataStorageConnectorConfig>>() {})
                .toInstance(metadataSpec);
          binder.bind(MetadataStorageTablesConfig.class).toInstance(metadataSpec.getMetadataStorageTablesConfig());
          binder.bind(IndexerMetadataStorageCoordinator.class).to(IndexerSQLMetadataStorageCoordinator.class).in(
              LazySingleton.class
          );
        }
    );
  }

  @Override
  public void run()
  {
    try {
      Injector injector = makeInjector();

      config = getHadoopDruidIndexerConfig();

      MetadataStorageUpdaterJobSpec metadataSpec = config.getSchema().getIOConfig().getMetadataUpdateSpec();
      // override metadata storage type based on HadoopIOConfig
      Preconditions.checkNotNull(metadataSpec.getType(), "type in metadataUpdateSpec must not be null");
      injector.getInstance(Properties.class).setProperty("druid.metadata.storage.type", metadataSpec.getType());

      config = HadoopDruidIndexerConfig.fromSpec(
          HadoopIngestionSpec.updateSegmentListIfDatasourcePathSpecIsUsed(
              config.getSchema(),
              HadoopDruidIndexerConfig.JSON_MAPPER,
              new MetadataStoreBasedUsedSegmentLister(
                  injector.getInstance(IndexerMetadataStorageCoordinator.class)
              )
          )
      );

      List<Jobby> jobs = new ArrayList<>();
      jobs.add(new HadoopDruidDetermineConfigurationJob(config));
      jobs.add(new HadoopDruidIndexerJob(config, injector.getInstance(MetadataStorageUpdaterJobHandler.class)));
      JobHelper.runJobs(jobs, config);

    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public HadoopDruidIndexerConfig getHadoopDruidIndexerConfig()
  {
    if (config == null) {
      try {
        if (argumentSpec.startsWith("{")) {
          config = HadoopDruidIndexerConfig.fromString(argumentSpec);
        } else {
          File localConfigFile = null;

          try {
            final URI argumentSpecUri = new URI(argumentSpec);
            final String argumentSpecScheme = argumentSpecUri.getScheme();

            if (argumentSpecScheme == null || "file".equals(argumentSpecScheme)) {
              // File URI.
              localConfigFile = new File(argumentSpecUri.getPath());
            }
          }
          catch (URISyntaxException e) {
            // Not a URI, assume it's a local file.
            localConfigFile = new File(argumentSpec);
          }

          if (localConfigFile != null) {
            config = HadoopDruidIndexerConfig.fromFile(localConfigFile);
          } else {
            config = HadoopDruidIndexerConfig.fromDistributedFileSystem(argumentSpec);
          }
        }
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return config;
  }
}
