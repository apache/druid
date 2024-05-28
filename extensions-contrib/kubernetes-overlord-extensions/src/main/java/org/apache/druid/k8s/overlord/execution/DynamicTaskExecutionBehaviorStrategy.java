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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Implements {@link ExecutionBehaviorStrategy} by dynamically evaluating a series of mapping rules.
 * Each rule corresponds to a potential task category.
 */
public class DynamicTaskExecutionBehaviorStrategy implements ExecutionBehaviorStrategy
{
  private LinkedHashMap<String, MappingRule> categoryRuleMap;

  /**
   * Constructs a new strategy with a predefined map of category rules.
   *
   * @param categoryRuleMap a map linking categories to their respective {@link MappingRule}
   */
  @JsonCreator
  public DynamicTaskExecutionBehaviorStrategy(
      @JsonProperty("categoryMap") LinkedHashMap<String, MappingRule> categoryRuleMap
  )
  {
    this.categoryRuleMap = categoryRuleMap;
  }

  /**
   * Evaluates the provided task against the set mapping rules to determine its category.
   *
   * @param task the task to be categorized
   * @return the category if a rule matches, otherwise null
   */
  @Override
  public String getTaskCategory(Task task)
  {
    return categoryRuleMap.entrySet()
                          .stream()
                          .filter(categoryRule ->
                                      categoryRule.getValue().evaluate(task))
                          .findFirst()
                          .map(Map.Entry::getKey)
                          .orElse(null);
  }

  @JsonProperty("categoryMap")
  public LinkedHashMap<String, MappingRule> getCategoryRuleMap()
  {
    return categoryRuleMap;
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
    DynamicTaskExecutionBehaviorStrategy that = (DynamicTaskExecutionBehaviorStrategy) o;
    return Objects.equals(categoryRuleMap, that.categoryRuleMap);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(categoryRuleMap);
  }

  @Override
  public String toString()
  {
    return "DynamicTaskExecutionBehaviorStrategy{" +
           "categoryMap=" + categoryRuleMap +
           '}';
  }
}
