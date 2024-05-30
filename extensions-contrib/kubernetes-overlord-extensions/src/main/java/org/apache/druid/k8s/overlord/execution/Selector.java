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

/**
 * Represents a condition-based selector that evaluates whether a given task meets specified criteria.
 * The selector uses conditions defined on context tags and task fields to determine if a task matches.
 */
public class Selector
{
  private final String name;
  private final Map<String, Set<String>> cxtTagsConditions;
  private final Map<String, Set<String>> taskFieldsConditions;

  /**
   * Creates a selector with specified conditions for context tags and task fields.
   *
   * @param name                 the identifier representing the outcome when a task matches the conditions
   * @param cxtTagsConditions    conditions on context tags
   * @param taskFieldsConditions conditions on task fields
   */
  @JsonCreator
  public Selector(
      @JsonProperty("name") String name,
      @JsonProperty("context.tags") Map<String, Set<String>> cxtTagsConditions,
      @JsonProperty("task") Map<String, Set<String>> taskFieldsConditions
  )
  {
    this.name = name;
    this.cxtTagsConditions = cxtTagsConditions;
    this.taskFieldsConditions = taskFieldsConditions;
  }

  /**
   * Evaluates this selector against a given task.
   *
   * @param task the task to evaluate
   * @return true if the task meets all the conditions specified by this selector, otherwise false
   */
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

  @JsonProperty
  public String getName()
  {
    return name;
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
    Selector selector = (Selector) o;
    return Objects.equals(name, selector.name) && Objects.equals(
        cxtTagsConditions,
        selector.cxtTagsConditions
    ) && Objects.equals(taskFieldsConditions, selector.taskFieldsConditions);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, cxtTagsConditions, taskFieldsConditions);
  }

  @Override
  public String toString()
  {
    return "Selector{" +
           "name=" + name +
           ", context.tags=" + cxtTagsConditions +
           ", task=" + taskFieldsConditions +
           '}';
  }
}
