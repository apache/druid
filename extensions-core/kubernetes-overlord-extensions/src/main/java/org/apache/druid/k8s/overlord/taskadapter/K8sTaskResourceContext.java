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

package org.apache.druid.k8s.overlord.taskadapter;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.k8s.overlord.common.DruidK8sConstants;

import java.util.HashMap;
import java.util.Map;

class K8sTaskResourceContext
{
  static final String REQUESTS_KEY = "requests";
  static final String LIMITS_KEY = "limits";
  static final String BY_TASK_TYPE_KEY = "byTaskType";

  static ResourceRequirements applyTaskResourceOverrides(ResourceRequirements requirements, Task task)
  {
    final Object rawResourceContext = task.getContextValue(DruidK8sConstants.TASK_CONTEXT_RESOURCES_KEY);
    if (rawResourceContext == null) {
      return requirements;
    }

    final Map<?, ?> resourceContext = asMap(rawResourceContext, DruidK8sConstants.TASK_CONTEXT_RESOURCES_KEY);
    final ResourceRequirements result = applyResourceConfig(
        requirements,
        resourceContext,
        DruidK8sConstants.TASK_CONTEXT_RESOURCES_KEY
    );

    final Object rawByTaskType = resourceContext.get(BY_TASK_TYPE_KEY);
    if (rawByTaskType == null) {
      return result;
    }

    final String taskTypePath = DruidK8sConstants.TASK_CONTEXT_RESOURCES_KEY + "." + BY_TASK_TYPE_KEY;
    final Map<?, ?> byTaskType = asMap(rawByTaskType, taskTypePath);
    final Object rawTaskTypeResources = byTaskType.get(task.getType());
    if (rawTaskTypeResources == null) {
      return result;
    }

    return applyResourceConfig(
        result,
        asMap(rawTaskTypeResources, taskTypePath + "." + task.getType()),
        taskTypePath + "." + task.getType()
    );
  }

  private static ResourceRequirements applyResourceConfig(
      ResourceRequirements requirements,
      Map<?, ?> resourceConfig,
      String contextPath
  )
  {
    final Map<String, Quantity> sharedResources = getSharedResources(resourceConfig, contextPath);
    final Map<String, Quantity> requestResources = getResourceMap(resourceConfig, REQUESTS_KEY, contextPath);
    final Map<String, Quantity> limitResources = getResourceMap(resourceConfig, LIMITS_KEY, contextPath);

    if (sharedResources.isEmpty() && requestResources.isEmpty() && limitResources.isEmpty()) {
      return requirements;
    }

    final ResourceRequirements result = requirements == null
                                        ? new ResourceRequirementsBuilder().build()
                                        : requirements;

    if (!sharedResources.isEmpty()) {
      addRequests(result, sharedResources);
      addLimits(result, sharedResources);
    }
    if (!requestResources.isEmpty()) {
      addRequests(result, requestResources);
    }
    if (!limitResources.isEmpty()) {
      addLimits(result, limitResources);
    }

    return result;
  }

  private static Map<String, Quantity> getSharedResources(Map<?, ?> resourceConfig, String contextPath)
  {
    final Map<String, Quantity> resources = new HashMap<>();
    for (Map.Entry<?, ?> entry : resourceConfig.entrySet()) {
      final String key = asStringKey(entry.getKey(), contextPath);
      if (REQUESTS_KEY.equals(key) || LIMITS_KEY.equals(key) || BY_TASK_TYPE_KEY.equals(key)) {
        continue;
      }
      resources.put(key, asQuantity(entry.getValue(), contextPath + "." + key));
    }
    return resources;
  }

  private static Map<String, Quantity> getResourceMap(Map<?, ?> resourceConfig, String resourceType, String contextPath)
  {
    final Object rawResources = resourceConfig.get(resourceType);
    if (rawResources == null) {
      return Map.of();
    }

    final String resourcePath = contextPath + "." + resourceType;
    final Map<?, ?> rawResourceMap = asMap(rawResources, resourcePath);
    final Map<String, Quantity> resources = new HashMap<>();
    for (Map.Entry<?, ?> entry : rawResourceMap.entrySet()) {
      final String key = asStringKey(entry.getKey(), resourcePath);
      resources.put(key, asQuantity(entry.getValue(), resourcePath + "." + key));
    }
    return resources;
  }

  private static void addRequests(ResourceRequirements requirements, Map<String, Quantity> resources)
  {
    if (requirements.getRequests() == null) {
      requirements.setRequests(new HashMap<>());
    }
    requirements.getRequests().putAll(resources);
  }

  private static void addLimits(ResourceRequirements requirements, Map<String, Quantity> resources)
  {
    if (requirements.getLimits() == null) {
      requirements.setLimits(new HashMap<>());
    }
    requirements.getLimits().putAll(resources);
  }

  private static Map<?, ?> asMap(Object value, String contextPath)
  {
    if (value instanceof Map) {
      return (Map<?, ?>) value;
    }

    throw InvalidInput.exception("Task context value [%s] must be an object.", contextPath);
  }

  private static String asStringKey(Object key, String contextPath)
  {
    if (key instanceof String) {
      return (String) key;
    }

    throw InvalidInput.exception("Task context value [%s] must contain only string keys.", contextPath);
  }

  private static Quantity asQuantity(Object value, String contextPath)
  {
    if (value instanceof Quantity) {
      return (Quantity) value;
    } else if (value instanceof String || value instanceof Number) {
      return new Quantity(String.valueOf(value));
    }

    throw InvalidInput.exception("Task context value [%s] must be a string or number.", contextPath);
  }
}
