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

package org.apache.druid.k8s.overlord.common;

import com.google.common.base.Optional;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.joda.time.Period;
import org.joda.time.PeriodType;


public class JobResponse
{

  private static final EmittingLogger LOGGER = new EmittingLogger(JobResponse.class);

  private final Job job;
  private final PeonPhase phase;

  public JobResponse(Job job, PeonPhase phase)
  {
    this.job = job;
    this.phase = phase;
  }

  public Job getJob()
  {
    return job;
  }

  public PeonPhase getPhase()
  {
    return phase;
  }

  public Optional<Long> getJobDuration()
  {
    Optional<Long> duration = Optional.absent();
    try {
      if (job.getStatus() != null
          && job.getStatus().getStartTime() != null
          && job.getStatus().getCompletionTime() != null) {
        duration = Optional.of((long) new Period(
            DateTimes.of(job.getStatus().getStartTime()),
            DateTimes.of(job.getStatus().getCompletionTime()),
            PeriodType.millis()
        ).getMillis());
      }
    }
    catch (Exception e) {
      LOGGER.error(e, "Error calculating duration for job: %s", job.getMetadata().getName());
    }
    if (duration.isPresent()) {
      LOGGER.info("Duration for Job: %s was %d seconds", job.getMetadata().getName(), duration.get());
    } else {
      LOGGER.info("Unable to calcuate duration for Job: %s", job.getMetadata().getName());
    }
    return duration;
  }
}
