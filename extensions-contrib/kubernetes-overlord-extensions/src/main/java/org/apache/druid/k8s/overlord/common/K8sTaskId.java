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

import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.indexing.common.task.Task;

import java.util.Locale;
import java.util.Objects;

public class K8sTaskId
{

  private final String k8sTaskId;
  private final String originalTaskId;

  public K8sTaskId(Task task)
  {
    this(task.getId());
  }

  public K8sTaskId(String taskId)
  {
    this.originalTaskId = taskId;
    // replace all the ": - . _" to "", try to reduce the length of pod name and meet pod naming specifications 64 characters.
    this.k8sTaskId = StringUtils.left(RegExUtils.replaceAll(taskId, "[^a-zA-Z0-9\\\\s]", "")
                                                .toLowerCase(Locale.ENGLISH), 63);
  }

  public String getK8sTaskId()
  {
    return k8sTaskId;
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
    return k8sTaskId.equals(k8sTaskId1.k8sTaskId) && originalTaskId.equals(k8sTaskId1.originalTaskId);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(k8sTaskId, originalTaskId);
  }

  @Override
  public String toString()
  {
    return "[ " + originalTaskId + ", " + k8sTaskId + "]";
  }
}
