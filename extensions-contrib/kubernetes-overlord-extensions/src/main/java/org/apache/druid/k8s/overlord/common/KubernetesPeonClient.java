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
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.batch.v1.Job;

import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A Kubernetes client wrapper to assist with peon task managment.
 * It provides a high level api to retreive jobs, launch jobs, delete jobs and various other
 * tasks like getting task logs, listing all active tasks.
 */
public interface KubernetesPeonClient
{

  Optional<Job> jobExists(K8sTaskId taskId);

  Pod launchJobAndWaitForStart(Job job, long howLong, TimeUnit timeUnit);

  JobResponse waitForJobCompletion(K8sTaskId taskId, long howLong, TimeUnit timeUnit);

  boolean cleanUpJob(K8sTaskId taskId);

  Optional<InputStream> getPeonLogs(K8sTaskId taskId);

  List<Job> listAllPeonJobs();

  List<Pod> listPeonPods(Set<PeonPhase> phases);

  List<Pod> listPeonPods();

  int cleanCompletedJobsOlderThan(long howFarBack, TimeUnit timeUnit);

  Pod getMainJobPod(K8sTaskId taskId);


}
