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
import java.util.Set;

public class MappingRule
{
  private List<Selector> selectors;

  @JsonCreator
  public MappingRule(@JsonProperty("selectors") List<Selector> selectors)
  {
    this.selectors = selectors;
  }

  public boolean evaluate(Task task)
  {
    return selectors.stream().anyMatch(selector -> selector.isSatisfied(task));
  }

  @JsonProperty
  public List<Selector> getSelectors()
  {
    return selectors;
  }

  static class Selector
  {
    private Map<String, Set<String>> cxtTagsConditions;
    private Map<String, Set<String>> taskFieldsConditions;

    @JsonCreator
    public Selector(
        @JsonProperty("context.tags") Map<String, Set<String>> cxtTagsConditions,
        @JsonProperty("task") Map<String, Set<String>> taskFieldsConditions
    )
    {
      this.cxtTagsConditions = cxtTagsConditions;
      this.taskFieldsConditions = taskFieldsConditions;
    }

    public boolean isSatisfied(Task task)
    {
      boolean tagsMatch = cxtTagsConditions.entrySet().stream().allMatch(entry -> {
        String expectedTag = entry.getKey();
        Set<String> expectedTagSet = entry.getValue();
        Map<String, Object> tags = task.getContextValue(DruidMetrics.TAGS);
        if (tags == null || tags.isEmpty()) {
          return false;
        }
        Object tagValue = tags.get(expectedTag);

        return tagValue == null ? false : expectedTagSet.contains((String) tagValue);
      });

      if (!tagsMatch) {
        return false;
      }

      return taskFieldsConditions.entrySet().stream().allMatch(entry -> {
        String expectedField = entry.getKey();
        Set<String> expectedFieldSet = entry.getValue();
        if ("datasource".equalsIgnoreCase(expectedField)) {
          return expectedFieldSet.contains(task.getDataSource());
        }

        if ("type".equalsIgnoreCase(expectedField)) {
          return expectedFieldSet.contains(task.getType());
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
  }
}
