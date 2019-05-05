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

import com.google.inject.Inject;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.ArrayList;
import java.util.List;

public class HadoopDruidBuildDictJob implements Jobby
{
  private static final Logger log = new Logger(HadoopDruidBuildDictJob.class);
  private final HadoopDruidIndexerConfig config;
  private final String zkHosts;
  private final String zkBase;
  private BuildDictJob dictJob;


  @Inject
  public HadoopDruidBuildDictJob(HadoopDruidIndexerConfig config, String zkHosts, String zkBase)
  {
    config.verify();
    this.config = config;
    this.zkHosts = zkHosts;
    this.zkBase = zkBase;
  }


  @Override
  public boolean run()
  {
    List<Jobby> jobs = new ArrayList<>();
    JobHelper.ensurePaths(config);

    dictJob = new BuildDictJob(config, zkHosts, zkBase);
    jobs.add(dictJob);


    return JobHelper.runJobs(jobs, config);
  }
}
