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

import java.util.List;
import java.util.Objects;

/**
 * Implements {@link ExecutionBehaviorStrategy} by dynamically evaluating a series of selectors.
 * Each selector corresponds to a potential task category.
 */
public class DynamicTaskExecutionBehaviorStrategy implements ExecutionBehaviorStrategy
{
  private List<Selector> categorySelectors;

  @JsonCreator
  public DynamicTaskExecutionBehaviorStrategy(
      @JsonProperty("categorySelectors") List<Selector> categorySelectors
  )
  {
    this.categorySelectors = categorySelectors;
  }

  /**
   * Evaluates the provided task against the set selectors to determine its category.
   *
   * @param task the task to be categorized
   * @return the category if a selector matches, otherwise null
   */
  @Override
  public String getTaskCategory(Task task)
  {
    return categorySelectors.stream()
                            .filter(selector -> selector.evaluate(task))
                            .findFirst()
                            .map(Selector::getName)
                            .orElse(null);
  }

  @JsonProperty
  public List<Selector> getCategorySelectors()
  {
    return categorySelectors;
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
    return Objects.equals(categorySelectors, that.categorySelectors);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(categorySelectors);
  }

  @Override
  public String toString()
  {
    return "DynamicTaskExecutionBehaviorStrategy{" +
           "categorySelectors=" + categorySelectors +
           '}';
  }
}
