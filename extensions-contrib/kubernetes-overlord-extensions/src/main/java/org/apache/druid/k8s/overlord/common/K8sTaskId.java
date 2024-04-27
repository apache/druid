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

import org.apache.druid.indexing.common.task.Task;

import java.util.Objects;

public class K8sTaskId
{

  private final String k8sJobName;
  private final String originalTaskId;

  public K8sTaskId(Task task)
  {
    this(task.getId());
  }

  public K8sTaskId(String taskId)
  {
    this.originalTaskId = taskId;
    this.k8sJobName = KubernetesOverlordUtils.convertTaskIdToJobName(taskId);
  }

  public String getK8sJobName()
  {
    return k8sJobName;
  }

  public String getOriginalTaskId()
  {
    return originalTaskId;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    K8sTaskId k8sTaskId1 = (K8sTaskId) o;
    return k8sJobName.equals(k8sTaskId1.k8sJobName) && originalTaskId.equals(k8sTaskId1.originalTaskId);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(k8sJobName, originalTaskId);
  }

  @Override
  public String toString()
  {
    return "[ " + originalTaskId + ", " + k8sJobName + "]";
  }
}
