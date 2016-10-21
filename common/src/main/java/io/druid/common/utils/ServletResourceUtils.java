/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.common.utils;

import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;
import java.util.Map;

public class ServletResourceUtils
{
  /**
   * Sanitize the exception as a map of "error" to information about the exception.
   *
   * This method explicitly suppresses the stack trace and any other logging. Any logging should be handled by the caller.
   * @param t The exception to sanitize
   * @return An immutable Map with a single entry which maps "error" to information about the error suitable for passing as an entity in a servlet error response.
   */
  public static Map<String, String> sanitizeException(@Nullable Throwable t)
  {
    return ImmutableMap.of(
        "error",
        t == null ? "null" : (t.getMessage() == null ? t.toString() : t.getMessage())
    );
  }
}
