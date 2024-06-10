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

package org.apache.druid.k8s.overlord.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.query.DruidMetrics;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TaskPropertiesMatcher implements Matcher
{
  private final Map<String, Set<String>> cxtTagsConditions;
  private final Map<String, Set<String>> taskFieldsConditions;

  /**
   * Creates a matcher with specified conditions for context tags and task fields.
   *
   * @param cxtTagsConditions    conditions on context tags
   * @param taskFieldsConditions conditions on task fields
   */
  @JsonCreator
  public TaskPropertiesMatcher(
      @JsonProperty("context.tags") Map<String, Set<String>> cxtTagsConditions,
      @JsonProperty("task") Map<String, Set<String>> taskFieldsConditions
  )
  {
    this.cxtTagsConditions = cxtTagsConditions;
    this.taskFieldsConditions = taskFieldsConditions;
  }

  @Override
  public boolean evaluate(Task task)
  {
    boolean tagsMatch = true;
    if (cxtTagsConditions != null) {
      tagsMatch = cxtTagsConditions.entrySet().stream().allMatch(entry -> {
        String tagKey = entry.getKey();
        Set<String> tagValues = entry.getValue();
        Map<String, Object> tags = task.getContextValue(DruidMetrics.TAGS);
        if (tags == null || tags.isEmpty()) {
          return false;
        }
        Object tagValue = tags.get(tagKey);

        return tagValue == null ? false : tagValues.contains((String) tagValue);
      });
    }

    if (!tagsMatch) {
      return false;
    }

    if (taskFieldsConditions != null) {
      return taskFieldsConditions.entrySet().stream().allMatch(entry -> {
        String fieldKey = entry.getKey();
        Set<String> fieldValues = entry.getValue();
        if ("datasource".equalsIgnoreCase(fieldKey)) {
          return fieldValues.contains(task.getDataSource());
        }

        if ("type".equalsIgnoreCase(fieldKey)) {
          return fieldValues.contains(task.getType());
        }

        return false;
      });
    }

    return true;
  }

  @JsonProperty("context.tags")
  public Map<String, Set<String>> getCxtTagsConditions()
  {
    return cxtTagsConditions;
  }

  @JsonProperty("task")
  public Map<String, Set<String>> getTaskFieldsConditions()
  {
    return taskFieldsConditions;
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
    TaskPropertiesMatcher matcher = (TaskPropertiesMatcher) o;
    return Objects.equals(cxtTagsConditions, matcher.cxtTagsConditions
    ) && Objects.equals(taskFieldsConditions, matcher.taskFieldsConditions);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(cxtTagsConditions, taskFieldsConditions);
  }

  @Override
  public String toString()
  {
    return "TaskPropertiesMatcher{" +
           "context.tags=" + cxtTagsConditions +
           ", task=" + taskFieldsConditions +
           '}';
  }
}
