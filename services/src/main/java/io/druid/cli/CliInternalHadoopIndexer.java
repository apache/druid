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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.druid.guice.LazySingleton;
import io.druid.indexer.HadoopDruidDetermineConfigurationJob;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.HadoopDruidIndexerJob;
import io.druid.indexer.HadoopIngestionSpec;
import io.druid.indexer.JobHelper;
import io.druid.indexer.Jobby;
import io.druid.indexer.MetadataStorageUpdaterJobHandler;
import io.druid.indexer.path.MetadataStoreBasedUsedSegmentLister;
import io.druid.indexer.updater.MetadataStorageUpdaterJobSpec;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.metadata.MetadataStorageTablesConfig;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;

/**
 */
@Command(
    name = "hadoop-indexer",
    description = "Runs the batch Hadoop Druid Indexer, see http://druid.io/docs/latest/Batch-ingestion.html for a description."
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
    return ImmutableList.<Module>of(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
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

      List<Jobby> jobs = Lists.newArrayList();
      jobs.add(new HadoopDruidDetermineConfigurationJob(config));
      jobs.add(new HadoopDruidIndexerJob(config, injector.getInstance(MetadataStorageUpdaterJobHandler.class)));
      JobHelper.runJobs(jobs, config);

    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public HadoopDruidIndexerConfig getHadoopDruidIndexerConfig()
  {
    if(config == null) {
      try {
        if (argumentSpec.startsWith("{")) {
          config = HadoopDruidIndexerConfig.fromString(argumentSpec);
        } else {
          File localConfigFile = null;

          try {
            final URI argumentSpecUri = new URI(argumentSpec);
            final String argumentSpecScheme = argumentSpecUri.getScheme();

            if (argumentSpecScheme == null || argumentSpecScheme.equals("file")) {
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
        throw Throwables.propagate(e);
      }
    }
    return config;
  }
}
