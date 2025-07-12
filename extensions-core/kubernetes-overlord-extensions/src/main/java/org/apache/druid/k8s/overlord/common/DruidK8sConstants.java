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

package org.apache.druid.k8s.overlord.common;

import com.google.common.collect.ImmutableList;

public class DruidK8sConstants
{
  public static final String TASK = "task";
  public static final String TASK_ID = "task.id";
  public static final String TASK_TYPE = "task.type";
  public static final String TASK_GROUP_ID = "task.group.id";
  public static final String TASK_DATASOURCE = "task.datasource";
  public static final String TASK_JOB_TEMPLATE = "task.jobTemplate";
  public static final int PORT = 8100;
  public static final int TLS_PORT = 8091;
  public static final int DEFAULT_CPU_MILLICORES = 1000;
  public static final String DEFAULT_JAVA_HEAP_SIZE = "1G";
  public static final String TLS_ENABLED = "tls.enabled";
  public static final String TASK_JSON_ENV = "TASK_JSON";
  public static final String TASK_DIR_ENV = "TASK_DIR";
  public static final String TASK_ID_ENV = "TASK_ID";
  public static final String LOAD_BROADCAST_DATASOURCE_MODE_ENV = "LOAD_BROADCAST_DATASOURCE_MODE";
  public static final String LOAD_BROADCAST_SEGMENTS_ENV = "LOAD_BROADCAST_SEGMENTS";
  public static final String JAVA_OPTS = "JAVA_OPTS";
  public static final String DRUID_HOST_ENV = "druid_host";
  public static final String DRUID_HOSTNAME_ENV = "HOSTNAME";
  public static final String LABEL_KEY = "druid.k8s.peons";
  public static final String OVERLORD_NAMESPACE_KEY = "druid.overlord.namespace";
  public static final String DRUID_LABEL_PREFIX = "druid.";
  public static final String BASE_TEMPLATE_NAME = "base";
  public static final long MAX_ENV_VARIABLE_KBS = 130048; // 127 KB

  public static final ImmutableList<String> BLACKLISTED_PEON_POD_ERROR_MESSAGES = ImmutableList.of(
      // Catches limit to ratio constraint: https://github.com/kubernetes/kubernetes/blob/3e39d1074fc717a883aaf57b966dd7a06dfca2ec/plugin/pkg/admission/limitranger/admission.go#L359
      // {resource} max limit to request ratio per Container is {value}, but provided ratio is {value}
      "max limit to request ratio",
      // Catches exceeded quota error: https://github.com/kubernetes/kubernetes/blob/818b7ae68119c0932d7714502ce58f1b56606c8d/staging/src/k8s.io/apiserver/pkg/admission/plugin/resourcequota/controller.go#L622
      // exceeded quota: {value}, requested: {value}, used: {value}, limited: {value}
      "exceeded quota:"
  );
}
