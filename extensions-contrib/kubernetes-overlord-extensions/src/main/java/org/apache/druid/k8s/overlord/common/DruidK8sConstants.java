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

import com.google.common.base.Predicate;

public class DruidK8sConstants
{
  public static final String TASK_ID = "task.id";
  public static final int PORT = 8100;
  public static final int TLS_PORT = 8091;
  public static final String TLS_ENABLED = "tls.enabled";
  public static final String TASK_JSON_ENV = "TASK_JSON";
  public static final String TASK_DIR_ENV = "TASK_DIR";
  public static final String JAVA_OPTS = "JAVA_OPTS";
  public static final String DRUID_HOST_ENV = "druid_host";
  public static final String DRUID_HOSTNAME_ENV = "HOSTNAME";
  static final String LABEL_KEY = "druid.k8s.peons";
  static final Predicate<Throwable> IS_TRANSIENT = e -> e instanceof KubernetesResourceNotFoundException;
}
