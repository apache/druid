/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
import com.metamx.common.logger.Logger;
import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.druid.indexer.HadoopDruidDetermineConfigurationJob;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.HadoopDruidIndexerJob;
import io.druid.indexer.JobHelper;
import io.druid.indexer.Jobby;
import io.druid.indexer.MetadataStorageUpdaterJobHandler;
import io.druid.indexer.updater.MetadataStorageUpdaterJobSpec;
import io.druid.metadata.MetadataStorageConnectorConfig;

import java.io.File;
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

            // bind metadata storage config based on HadoopIOConfig
            MetadataStorageUpdaterJobSpec metadataSpec = getHadoopDruidIndexerConfig().getSchema()
                                                                                      .getIOConfig()
                                                                                      .getMetadataUpdateSpec();

            binder.bind(new TypeLiteral<Supplier<MetadataStorageConnectorConfig>>() {})
                  .toInstance(metadataSpec);
          }
        }
    );
  }

  @Override
  public void run()
  {
    try {
      Injector injector = makeInjector();

      MetadataStorageUpdaterJobSpec metadataSpec = getHadoopDruidIndexerConfig().getSchema().getIOConfig().getMetadataUpdateSpec();
      // override metadata storage type based on HadoopIOConfig
      Preconditions.checkNotNull(metadataSpec.getType(), "type in metadataUpdateSpec must not be null");
      injector.getInstance(Properties.class).setProperty("druid.metadata.storage.type", metadataSpec.getType());

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
          config = HadoopDruidIndexerConfig.fromFile(new File(argumentSpec));
        }
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
    return config;
  }
}
