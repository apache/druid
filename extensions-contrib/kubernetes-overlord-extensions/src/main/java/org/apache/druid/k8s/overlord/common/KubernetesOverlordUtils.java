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

import com.google.common.hash.Hashing;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.regex.Pattern;

public class KubernetesOverlordUtils
{
  private static final Pattern K8S_LABEL_PATTERN = Pattern.compile("[^A-Za-z0-9_.-]");
  // replace all the ": - . _" to "", try to reduce the length of pod name and meet pod naming specifications 64 characters.
  private static final Pattern K8S_TASK_ID_PATTERN = Pattern.compile("[^a-zA-Z0-9\\\\s]");

  public static String convertStringToK8sLabel(String rawString)
  {
    String trimmedString = rawString == null ? "" : RegExUtils.replaceAll(rawString, K8S_LABEL_PATTERN, "");
    return StringUtils.left(StringUtils.strip(trimmedString, "_.-"), 63);

  }

  public static String convertTaskIdToK8sLabel(String taskId)
  {
    return taskId == null ? "" : StringUtils.left(RegExUtils.replaceAll(taskId, K8S_TASK_ID_PATTERN, "")
        .toLowerCase(Locale.ENGLISH), 63);
  }

  public static String convertTaskIdToJobName(String taskId)
  {
    return taskId == null ? "" : StringUtils.left(RegExUtils.replaceAll(taskId, K8S_TASK_ID_PATTERN, "")
        .toLowerCase(Locale.ENGLISH), 30) + "-" + Hashing.murmur3_128().hashString(taskId, StandardCharsets.UTF_8);
  }
}
