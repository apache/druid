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

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import com.google.common.io.InputSupplier;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.druid.guice.LazySingleton;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.HadoopDruidIndexerJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 */
@Command(
    name = "hadoop",
    description = "Runs the batch Hadoop Druid Indexer, see https://github.com/metamx/druid/wiki/Batch-ingestion for a description."
)
public class CliHadoopIndexer extends GuiceRunnable
{
  @Arguments(description = "A JSON object or the path to a file that contains a JSON object", required = true)
  private String argumentSpec;

  private static final Logger log = new Logger(CliHadoopIndexer.class);

  public CliHadoopIndexer()
  {
    super(log);
  }

  @Override
  protected List<Object> getModules()
  {
    return ImmutableList.<Object>of(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bind(HadoopDruidIndexerJob.class).in(LazySingleton.class);
          }

          @Provides
          @LazySingleton
          public HadoopDruidIndexerConfig getHadoopDruidIndexerConfig()
          {
            try {
              if (argumentSpec.startsWith("{")) {
                return HadoopDruidIndexerConfig.fromString(argumentSpec);
              } else if (argumentSpec.startsWith("s3://")) {
                final Path s3nPath = new Path(String.format("s3n://%s", argumentSpec.substring("s3://".length())));
                final FileSystem fs = s3nPath.getFileSystem(new Configuration());

                String configString = CharStreams.toString(
                    new InputSupplier<InputStreamReader>()
                    {
                      @Override
                      public InputStreamReader getInput() throws IOException
                      {
                        return new InputStreamReader(fs.open(s3nPath));
                      }
                    }
                );

                return HadoopDruidIndexerConfig.fromString(configString);
              } else {
                return HadoopDruidIndexerConfig.fromFile(new File(argumentSpec));
              }
            }
            catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
        }
    );
  }

  @Override
  public void run()
  {
    try {
      Injector injector = makeInjector();
      final HadoopDruidIndexerJob job = injector.getInstance(HadoopDruidIndexerJob.class);

      Lifecycle lifecycle = initLifecycle(injector);

      job.run();

      try {
        lifecycle.stop();
      }
      catch (Throwable t) {
        log.error(t, "Error when stopping. Failing.");
        System.exit(1);
      }

    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

}