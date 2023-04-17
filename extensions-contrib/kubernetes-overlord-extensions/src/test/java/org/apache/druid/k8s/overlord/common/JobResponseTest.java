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
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class JobResponseTest
{

  @Test
  void testCompletionTime()
  {
    Job job = mock(Job.class);
    ObjectMeta metadata = mock(ObjectMeta.class);
    when(metadata.getName()).thenReturn("job");
    when(job.getMetadata()).thenReturn(metadata);
    JobStatus jobStatus = mock(JobStatus.class);
    when(jobStatus.getStartTime()).thenReturn("2022-09-19T23:31:50Z");
    when(jobStatus.getCompletionTime()).thenReturn("2022-09-19T23:32:48Z");
    when(job.getStatus()).thenReturn(jobStatus);
    JobResponse response = new JobResponse(job, PeonPhase.SUCCEEDED);
    Optional<Long> duration = response.getJobDuration();
    Assertions.assertEquals(Long.valueOf(58000L), duration.get());
  }

  @Test
  void testNoDuration()
  {
    Job job = mock(Job.class);
    ObjectMeta metadata = mock(ObjectMeta.class);
    when(metadata.getName()).thenReturn("job");
    when(job.getMetadata()).thenReturn(metadata);
    JobStatus jobStatus = mock(JobStatus.class);
    when(jobStatus.getStartTime()).thenReturn("2022-09-19T23:31:50Z");
    when(job.getStatus()).thenReturn(jobStatus);
    JobResponse response = new JobResponse(job, PeonPhase.SUCCEEDED);
    Optional<Long> duration = response.getJobDuration();
    Assertions.assertFalse(duration.isPresent());
  }

  @Test
  void testMakingCodeCoverageHappy()
  {
    Job job = mock(Job.class);
    ObjectMeta metadata = mock(ObjectMeta.class);
    when(metadata.getName()).thenReturn("job");
    when(job.getMetadata()).thenReturn(metadata);
    when(job.getStatus()).thenReturn(null);
    JobResponse response = new JobResponse(job, PeonPhase.SUCCEEDED);
    Optional<Long> duration = response.getJobDuration();
    Assertions.assertFalse(duration.isPresent());
  }

  @Test
  void testNullJob()
  {
    JobResponse response = new JobResponse(null, PeonPhase.SUCCEEDED);
    Optional<Long> duration = response.getJobDuration();
    Assertions.assertFalse(duration.isPresent());
  }
}
