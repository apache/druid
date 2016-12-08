/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord.routing;

import io.druid.indexing.common.TaskStatus;

/**
 * A TaskStatusReporter is responsible for pushing task results back up the chain of custody.
 * It is expected that results reporting is idempotent.
 */

public interface TaskStatusReporter
{
  /**
   * Report the status of the task. The intended order of status reports is not specified. Any TaskStatusReporter
   * is expected to return TRUE if the status reports do not occur in order.
   *
   * @param status The TaskStatus to report
   *
   * @return True if the report succeeded. Specifically, a result of true indicates that the receiving end has heard
   * the report and further attempts to report the same status will be idempotent and not necessary. A value of true
   * does NOT indicate that the status has ben SET for the task, but rather that the status notification has been
   * handled in a highly available way. A value of FALSE indicates that something isn't right and the caller should
   * try again, preferably checking making sure that this method is called on the leader
   */
  boolean reportStatus(TaskStatus status);
}
