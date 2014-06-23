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
import com.google.api.client.util.Lists;
import com.metamx.common.logger.Logger;
import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.druid.indexer.HadoopDruidDetermineConfigurationJob;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.HadoopDruidIndexerJob;
import io.druid.indexer.HadoopIngestionSpec;
import io.druid.indexer.JobHelper;
import io.druid.indexer.Jobby;

import java.io.File;
import java.util.List;

/**
 */
@Command(
    name = "hadoop-indexer",
    description = "Runs the batch Hadoop Druid Indexer, see http://druid.io/docs/latest/Batch-ingestion.html for a description."
)
public class CliInternalHadoopIndexer implements Runnable
{
  private static final Logger log = new Logger(CliHadoopIndexer.class);
  @Arguments(description = "A JSON object or the path to a file that contains a JSON object", required = true)
  private String argumentSpec;

  @Override
  public void run()
  {
    try {
      HadoopDruidIndexerConfig config = getHadoopDruidIndexerConfig();
      List<Jobby> jobs = Lists.newArrayList();
      jobs.add(new HadoopDruidDetermineConfigurationJob(config));
      jobs.add(new HadoopDruidIndexerJob(config));
      JobHelper.runJobs(jobs, config);

    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public HadoopDruidIndexerConfig getHadoopDruidIndexerConfig()
  {
    try {
      HadoopIngestionSpec spec;
      if (argumentSpec.startsWith("{")) {
        return HadoopDruidIndexerConfig.fromString(argumentSpec);
      } else {
        return HadoopDruidIndexerConfig.fromFile(new File(argumentSpec));
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
