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

package org.apache.druid.k8s.overlord;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Binder;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.k8s.overlord.common.DruidK8sConstants;
import org.apache.druid.server.emitter.ExtraServiceDimensions;

import javax.annotation.Nullable;

/**
 * Tags every metric a task pod emits with the pod template it runs under, so metrics can be grouped
 * by pod template alongside the task dimensions the peon already reports.
 * <p>
 * The template name arrives in {@link DruidK8sConstants#POD_TEMPLATE_ENV}, which
 * {@link org.apache.druid.k8s.overlord.taskadapter.PodTemplateTaskAdapter} populates from the pod's
 * own {@link DruidK8sConstants#TASK_JOB_TEMPLATE} annotation. Peons launched without a pod template,
 * such as those from another task adapter, leave the variable unset and emit no extra dimension.
 */
@LoadScope(roles = NodeRole.PEON_JSON_NAME)
public class KubernetesPeonMetricsModule implements DruidModule
{
  private static final Logger log = new Logger(KubernetesPeonMetricsModule.class);

  @Override
  public void configure(Binder binder)
  {
    final MapBinder<String, String> extraServiceDimensions = MapBinder.newMapBinder(
        binder,
        String.class,
        String.class,
        ExtraServiceDimensions.class
    );

    final String podTemplate = getPodTemplateName();
    if (podTemplate == null || podTemplate.isEmpty()) {
      log.debug(
          "Env variable [%s] is not set, so metrics will not carry a [%s] dimension.",
          DruidK8sConstants.POD_TEMPLATE_ENV,
          DruidK8sConstants.POD_TEMPLATE_DIMENSION
      );
      return;
    }

    log.info("Emitting metrics with dimension [%s] set to [%s].", DruidK8sConstants.POD_TEMPLATE_DIMENSION, podTemplate);
    extraServiceDimensions.addBinding(DruidK8sConstants.POD_TEMPLATE_DIMENSION).toInstance(podTemplate);
  }

  /**
   * Overridden by tests, which cannot set an environment variable on the running process.
   */
  @VisibleForTesting
  @Nullable
  String getPodTemplateName()
  {
    return System.getenv(DruidK8sConstants.POD_TEMPLATE_ENV);
  }
}
