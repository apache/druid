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

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 */
public class HadoopDruidIndexerJob implements Jobby
{
  private static final Logger log = new Logger(HadoopDruidIndexerJob.class);
  private final HadoopDruidIndexerConfig config;
  @Nullable
  private final MetadataStorageUpdaterJob metadataStorageUpdaterJob;
  @Nullable
  private IndexGeneratorJob indexJob;
  @Nullable
  private volatile List<DataSegment> publishedSegments = null;
  @Nullable
  private String hadoopJobIdFile;

  @Inject
  public HadoopDruidIndexerJob(
      HadoopDruidIndexerConfig config,
      MetadataStorageUpdaterJobHandler handler
  )
  {
    config.verify();
    this.config = config;

    Preconditions.checkArgument(
        !config.isUpdaterJobSpecSet() || handler != null,
        "MetadataStorageUpdaterJobHandler must not be null if ioConfig.metadataUpdateSpec is specified."
    );

    if (config.isUpdaterJobSpecSet()) {
      metadataStorageUpdaterJob = new MetadataStorageUpdaterJob(
          config,
          handler
      );
    } else {
      metadataStorageUpdaterJob = null;
    }
  }

  @Override
  public boolean run()
  {
    List<Jobby> jobs = new ArrayList<>();
    JobHelper.ensurePaths(config);

    indexJob = new IndexGeneratorJob(config);
    jobs.add(indexJob);

    if (metadataStorageUpdaterJob != null) {
      jobs.add(metadataStorageUpdaterJob);
    } else {
      log.info(
          "No metadataStorageUpdaterJob set in the config. This is cool if you are running a hadoop index task, otherwise nothing will be uploaded to database."
      );
    }

    jobs.add(
        new Jobby()
        {
          @Override
          public boolean run()
          {
            publishedSegments = IndexGeneratorJob.getPublishedSegments(config);
            return true;
          }
        }
    );

    config.setHadoopJobIdFileName(hadoopJobIdFile);
    return JobHelper.runJobs(jobs, config);
  }

  @Override
  public Map<String, Object> getStats()
  {
    if (indexJob == null) {
      return null;
    }

    return indexJob.getStats();
  }

  @Nullable
  @Override
  public String getErrorMessage()
  {
    if (indexJob == null) {
      return null;
    }

    return indexJob.getErrorMessage();
  }

  public List<DataSegment> getPublishedSegments()
  {
    if (publishedSegments == null) {
      throw new IllegalStateException("Job hasn't run yet. No segments have been published yet.");
    }
    return publishedSegments;
  }

  public void setHadoopJobIdFile(String hadoopJobIdFile)
  {
    this.hadoopJobIdFile = hadoopJobIdFile;
  }
}
