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

import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatusBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JobStatusTest
{
  @Test
  void testJobsActive()
  {
    Assertions.assertFalse(JobStatus.isActive(null));
    Assertions.assertFalse(JobStatus.isActive(new JobBuilder().build()));
    Assertions.assertFalse(JobStatus.isActive(new JobBuilder().withStatus(new JobStatusBuilder().withActive(null).build()).build()));
    Assertions.assertFalse(JobStatus.isActive(new JobBuilder().withStatus(new JobStatusBuilder().withActive(0).build()).build()));
    Assertions.assertTrue(JobStatus.isActive(new JobBuilder().withStatus(new JobStatusBuilder().withActive(1).build()).build()));
  }

  @Test
  void testJobsSucceeded()
  {
    Assertions.assertFalse(JobStatus.isSucceeded(null));
    Assertions.assertFalse(JobStatus.isSucceeded(new JobBuilder().build()));
    Assertions.assertFalse(JobStatus.isSucceeded(new JobBuilder().withStatus(new JobStatusBuilder().withSucceeded(null).build()).build()));
    Assertions.assertFalse(JobStatus.isSucceeded(new JobBuilder().withStatus(new JobStatusBuilder().withSucceeded(0).build()).build()));
    Assertions.assertTrue(JobStatus.isSucceeded(new JobBuilder().withStatus(new JobStatusBuilder().withSucceeded(1).build()).build()));
  }

  @Test
  void testJobsFailed()
  {
    Assertions.assertFalse(JobStatus.isFailed(null));
    Assertions.assertFalse(JobStatus.isFailed(new JobBuilder().build()));
    Assertions.assertFalse(JobStatus.isFailed(new JobBuilder().withStatus(new JobStatusBuilder().withFailed(null).build()).build()));
    Assertions.assertFalse(JobStatus.isFailed(new JobBuilder().withStatus(new JobStatusBuilder().withFailed(0).build()).build()));
    Assertions.assertTrue(JobStatus.isFailed(new JobBuilder().withStatus(new JobStatusBuilder().withFailed(1).build()).build()));
  }
}
