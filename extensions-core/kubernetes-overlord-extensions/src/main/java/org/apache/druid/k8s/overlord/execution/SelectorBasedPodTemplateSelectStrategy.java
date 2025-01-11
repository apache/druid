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
import com.google.common.base.Preconditions;
import io.fabric8.kubernetes.api.model.PodTemplate;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.k8s.overlord.common.DruidK8sConstants;
import org.apache.druid.k8s.overlord.taskadapter.PodTemplateWithName;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Implements {@link PodTemplateSelectStrategy} by dynamically evaluating a series of selectors.
 * Each selector corresponds to a potential task template key.
 */
public class SelectorBasedPodTemplateSelectStrategy implements PodTemplateSelectStrategy
{
  private final List<Selector> selectors;

  @JsonCreator
  public SelectorBasedPodTemplateSelectStrategy(
      @JsonProperty("selectors") List<Selector> selectors
  )
  {
    Preconditions.checkNotNull(selectors, "selectors");
    this.selectors = selectors;
  }

  /**
   * Evaluates the provided task against the set selectors to determine its template.
   *
   * @param task the task to be checked
   * @return the template if a selector matches, otherwise fallback to base template
   */
  @Override
  public PodTemplateWithName getPodTemplateForTask(Task task, Map<String, PodTemplate> templates)
  {
    String templateKey = selectors.stream()
                                  .filter(selector -> selector.evaluate(task))
                                  .findFirst()
                                  .map(Selector::getSelectionKey)
                                  .orElse(DruidK8sConstants.BASE_TEMPLATE_NAME);

    if (!templates.containsKey(templateKey)) {
      templateKey = DruidK8sConstants.BASE_TEMPLATE_NAME;
    }
    return new PodTemplateWithName(templateKey, templates.get(templateKey));
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
    SelectorBasedPodTemplateSelectStrategy that = (SelectorBasedPodTemplateSelectStrategy) o;
    return Objects.equals(selectors, that.selectors);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(selectors);
  }

  @Override
  public String toString()
  {
    return "SelectorBasedPodTemplateSelectStrategy{" +
           "selectors=" + selectors +
           '}';
  }
}
