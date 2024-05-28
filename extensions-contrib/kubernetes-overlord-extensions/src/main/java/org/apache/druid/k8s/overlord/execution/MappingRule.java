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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a rule that can evaluate whether a task satisfies a set of conditions.
 */
public class MappingRule
{
  private List<Selector> selectors;

  @JsonCreator
  public MappingRule(@JsonProperty("selectors") List<Selector> selectors)
  {
    this.selectors = selectors;
  }

  /**
   * Evaluates the specified task against all selectors, determining if any selector is satisfied by the task.
   *
   * @param task the task to evaluate
   * @return true if any selector is satisfied, otherwise false
   */
  public boolean evaluate(Task task)
  {
    return selectors.stream().anyMatch(selector -> selector.isSatisfied(task));
  }

  @JsonProperty
  public List<Selector> getSelectors()
  {
    return selectors;
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
    MappingRule that = (MappingRule) o;
    return Objects.equals(selectors, that.selectors);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(selectors);
  }

  @Override
  public String toString()
  {
    return "MappingRule{" +
           "selectors=" + selectors +
           '}';
  }

  /**
   * Represents a condition or set of conditions that can be used to evaluate whether a task meets specific criteria.
   */
  static class Selector
  {
    private Map<String, Set<String>> cxtTagsConditions;
    private Map<String, Set<String>> taskFieldsConditions;

    /**
     * Creates a selector with specified conditions for context tags and task fields.
     *
     * @param cxtTagsConditions conditions on context tags
     * @param taskFieldsConditions conditions on task fields
     */
    @JsonCreator
    public Selector(
        @JsonProperty("context.tags") Map<String, Set<String>> cxtTagsConditions,
        @JsonProperty("task") Map<String, Set<String>> taskFieldsConditions
    )
    {
      this.cxtTagsConditions = cxtTagsConditions;
      this.taskFieldsConditions = taskFieldsConditions;
    }

    /**
     * Evaluates this selector against a given task.
     *
     * @param task the task to evaluate
     * @return true if the task meets all the conditions specified by this selector, otherwise false
     */
    public boolean isSatisfied(Task task)
    {
      boolean tagsMatch = cxtTagsConditions.entrySet().stream().anyMatch(entry -> {
        String expectedTag = entry.getKey();
        Set<String> expectedTagSet = entry.getValue();
        Map<String, Object> tags = task.getContextValue(DruidMetrics.TAGS);
        if (tags == null || tags.isEmpty()) {
          return false;
        }
        Object tagValue = tags.get(expectedTag);

        return tagValue == null ? false : expectedTagSet.contains((String) tagValue);
      });

      if (tagsMatch) {
        return true;
      }

      return taskFieldsConditions.entrySet().stream().anyMatch(entry -> {
        String expectedField = entry.getKey();
        Set<String> expectedValueSet = entry.getValue();
        if ("datasource".equalsIgnoreCase(expectedField)) {
          return expectedValueSet.contains(task.getDataSource());
        }

        if ("type".equalsIgnoreCase(expectedField)) {
          return expectedValueSet.contains(task.getType());
        }

        return false;
      });
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
      return Objects.equals(cxtTagsConditions, selector.cxtTagsConditions) && Objects.equals(
          taskFieldsConditions,
          selector.taskFieldsConditions
      );
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(cxtTagsConditions, taskFieldsConditions);
    }

    @Override
    public String toString()
    {
      return "Selector{" +
             "context.tags=" + cxtTagsConditions +
             ", task=" + taskFieldsConditions +
             '}';
    }
  }
}
