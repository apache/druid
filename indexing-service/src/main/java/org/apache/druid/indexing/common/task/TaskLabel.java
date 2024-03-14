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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;

/**
 * Represents a container for labels associated with a task. These labels are intended to categorize
 * tasks based on various characteristics or operational roles within the system. This class provides
 * a structured way to manage and access these labels, facilitating task management and scheduling
 * processes.
 *
 * Labels are used to enrich tasks with metadata that can influence scheduling decisions, resource allocation,
 * and more, depending on their interpretation by the system.
 */
public class TaskLabel
{
  @JsonProperty
  private final List<String> labels;

  @JsonCreator
  @VisibleForTesting
  public TaskLabel(
      @JsonProperty("labels") List<String> labels
  )
  {
    this.labels = labels;
    Preconditions.checkNotNull(labels, "labels list cannot be null.");
  }

  @JsonProperty
  public List<String> getLabels()
  {
    return labels;
  }

  @Override
  public String toString()
  {
    return "TaskLabel{" +
           "labels=" + labels +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TaskLabel)) {
      return false;
    }
    TaskLabel taskLabel = (TaskLabel) o;
    return Objects.equals(labels, taskLabel.labels);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(labels);
  }
}
