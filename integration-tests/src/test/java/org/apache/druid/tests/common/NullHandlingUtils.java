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

package org.apache.druid.tests.common;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;

import java.util.function.Function;

public class NullHandlingUtils
{
  public static Function<String, String> IS_NOT_NULL_TRANSFORM_FOR_SQL = template -> StringUtils.replace(
      template,
      "%%IS_NOT_NULL%%",
      NullHandling.sqlCompatible() ? "IS NOT NULL" : "<> ''"
  );

  private NullHandlingUtils()
  {
  }
}
