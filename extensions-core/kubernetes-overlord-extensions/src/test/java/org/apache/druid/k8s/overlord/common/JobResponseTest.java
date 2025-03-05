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

import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class JobResponseTest
{
  @Test
  void testCompletionTime()
  {
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName("job")
        .endMetadata()
        .withNewStatus()
        .withStartTime("2022-09-19T23:31:50Z")
        .withCompletionTime("2022-09-19T23:32:48Z")
        .endStatus()
        .build();

    JobResponse response = new JobResponse(job, PeonPhase.SUCCEEDED);

    Assertions.assertEquals(58000L, response.getJobDuration());
  }

  @Test
  void testNoDuration()
  {
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName("job")
        .endMetadata()
        .withNewStatus()
        .withStartTime("2022-09-19T23:31:50Z")
        .endStatus()
        .build();

    JobResponse response = new JobResponse(job, PeonPhase.SUCCEEDED);

    Assertions.assertEquals(-1, response.getJobDuration());
  }

  @Test
  void testMakingCodeCoverageHappy()
  {
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName("job")
        .endMetadata()
        .build();

    JobResponse response = new JobResponse(job, PeonPhase.SUCCEEDED);

    Assertions.assertEquals(-1, response.getJobDuration());
  }

  @Test
  void testNullJob()
  {
    JobResponse response = new JobResponse(null, PeonPhase.SUCCEEDED);
    long duration = response.getJobDuration();
    Assertions.assertEquals(-1, duration);
  }
}
