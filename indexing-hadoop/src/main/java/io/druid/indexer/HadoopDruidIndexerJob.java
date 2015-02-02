/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexer;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.timeline.DataSegment;

import java.util.List;

/**
 */
public class HadoopDruidIndexerJob implements Jobby
{
  private static final Logger log = new Logger(HadoopDruidIndexerJob.class);
  private final HadoopDruidIndexerConfig config;
  private final DbUpdaterJob dbUpdaterJob;
  private IndexGeneratorJob indexJob;
  private volatile List<DataSegment> publishedSegments = null;

  @Inject
  public HadoopDruidIndexerJob(
      HadoopDruidIndexerConfig config
  )
  {
    config.verify();
    this.config = config;

    if (config.isUpdaterJobSpecSet()) {
      dbUpdaterJob = new DbUpdaterJob(config);
    } else {
      dbUpdaterJob = null;
    }
  }

  @Override
  public boolean run()
  {
    List<Jobby> jobs = Lists.newArrayList();
    JobHelper.ensurePaths(config);

    indexJob = new IndexGeneratorJob(config);
    jobs.add(indexJob);

    if (dbUpdaterJob != null) {
      jobs.add(dbUpdaterJob);
    } else {
      log.info("No updaterJobSpec set, not uploading to database");
    }

    jobs.add(new Jobby()
    {
      @Override
      public boolean run()
      {
        publishedSegments = IndexGeneratorJob.getPublishedSegments(config);
        return true;
      }
    });


    JobHelper.runJobs(jobs, config);
    return true;
  }

  public List<DataSegment> getPublishedSegments()
  {
    if (publishedSegments == null) {
      throw new IllegalStateException("Job hasn't run yet. No segments have been published yet.");
    }
    return publishedSegments;
  }

  public IndexGeneratorJob.IndexGeneratorStats getIndexJobStats()
  {
    return indexJob.getJobStats();
  }
}
