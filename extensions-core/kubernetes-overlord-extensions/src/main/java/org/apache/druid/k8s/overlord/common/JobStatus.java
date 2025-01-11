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

public class JobStatus
{

  public static boolean isActive(Job job)
  {
    if (job == null || job.getStatus() == null || job.getStatus().getActive() == null) {
      return false;
    }
    return job.getStatus().getActive() > 0;
  }

  public static boolean isSucceeded(Job job)
  {
    if (job == null || job.getStatus() == null || job.getStatus().getSucceeded() == null) {
      return false;
    }
    return job.getStatus().getSucceeded() > 0;
  }

  public static boolean isFailed(Job job)
  {
    if (job == null || job.getStatus() == null || job.getStatus().getFailed() == null) {
      return false;
    }
    return job.getStatus().getFailed() > 0;
  }
}
