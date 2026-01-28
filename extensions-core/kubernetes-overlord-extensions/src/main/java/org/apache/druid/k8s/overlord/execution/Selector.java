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
import org.apache.druid.utils.CollectionUtils;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a condition-based selector that evaluates whether a given task meets specified criteria.
 * The selector uses conditions defined on context tags and task fields to determine if a task matches.
 */
public class Selector
{
  private final String selectionKey;
  private final Map<String, Set<String>> cxtTagsConditions;
  private final Set<String> taskTypeCondition;
  private final Set<String> dataSourceCondition;

  /**
   * Creates a selector with specified conditions for context tags and task fields.
   *
   * @param selectionKey        the identifier representing the outcome when a task matches the conditions
   * @param cxtTagsConditions   conditions on context tags
   * @param taskTypeCondition   conditions on task type
   * @param dataSourceCondition conditions on task dataSource
   */
  @JsonCreator
  public Selector(
      @JsonProperty("selectionKey") String selectionKey,
      @JsonProperty("context.tags") Map<String, Set<String>> cxtTagsConditions,
      @JsonProperty("type") Set<String> taskTypeCondition,
      @JsonProperty("dataSource") Set<String> dataSourceCondition
  )
  {
    this.selectionKey = selectionKey;
    this.cxtTagsConditions = cxtTagsConditions;
    this.taskTypeCondition = taskTypeCondition;
    this.dataSourceCondition = dataSourceCondition;
  }

  /**
   * Evaluates this selector against a given task.
   *
   * @param task the task to evaluate
   * @return true if the task meets all the conditions specified by this selector, otherwise false
   */
  public boolean evaluate(Task task)
  {
    boolean isMatch = true;

    if (cxtTagsConditions != null) {
      isMatch = cxtTagsConditions.entrySet().stream().allMatch(entry -> {
        String tagKey = entry.getKey();
        Set<String> tagValues = entry.getValue();
        Map<String, Object> tags = task.getContextValue(DruidMetrics.TAGS);
        if (tags == null || tags.isEmpty()) {
          return false;
        }
        Object tagValue = tags.get(tagKey);

        return tagValue != null && tagValues.contains((String) tagValue);
      });
    }

    if (isMatch && !CollectionUtils.isNullOrEmpty(taskTypeCondition)) {
      isMatch = taskTypeCondition.contains(task.getType());
    }

    if (isMatch && !CollectionUtils.isNullOrEmpty(dataSourceCondition)) {
      isMatch = dataSourceCondition.contains(task.getDataSource());
    }

    return isMatch;
  }

  @JsonProperty
  public String getSelectionKey()
  {
    return selectionKey;
  }

  @JsonProperty("context.tags")
  public Map<String, Set<String>> getCxtTagsConditions()
  {
    return cxtTagsConditions;
  }

  @JsonProperty("type")
  public Set<String> getTaskTypeCondition()
  {
    return taskTypeCondition;
  }

  @JsonProperty("dataSource")
  public Set<String> getDataSourceCondition()
  {
    return dataSourceCondition;
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
    return Objects.equals(selectionKey, selector.selectionKey) && Objects.equals(
        cxtTagsConditions,
        selector.cxtTagsConditions
    ) && Objects.equals(taskTypeCondition, selector.taskTypeCondition) && Objects.equals(
        dataSourceCondition,
        selector.dataSourceCondition
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(selectionKey, cxtTagsConditions, taskTypeCondition, dataSourceCondition);
  }

  @Override
  public String toString()
  {
    return "Selector{" +
           "selectionKey=" + selectionKey +
           ", context.tags=" + cxtTagsConditions +
           ", type=" + taskTypeCondition +
           ", dataSource=" + dataSourceCondition +
           '}';
  }
}
