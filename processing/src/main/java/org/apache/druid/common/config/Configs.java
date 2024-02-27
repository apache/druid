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

package org.apache.druid.common.config;

/**
 * Utility class for common config operations.
 */
public class Configs
{
  /**
   * Returns the given {@code value} if it is not null, otherwise returns the
   * {@code defaultValue}.
   */
  public static long valueOrDefault(Long value, long defaultValue)
  {
    return value == null ? defaultValue : value;
  }

  /**
   * Returns the given {@code value} if it is not null, otherwise returns the
   * {@code defaultValue}.
   */
  public static int valueOrDefault(Integer value, int defaultValue)
  {
    return value == null ? defaultValue : value;
  }

  /**
   * Returns the given {@code value} if it is not null, otherwise returns the
   * {@code defaultValue}.
   */
  public static boolean valueOrDefault(Boolean value, boolean defaultValue)
  {
    return value == null ? defaultValue : value;
  }

  /**
   * Returns the given {@code value} if it is not null, otherwise returns the
   * {@code defaultValue}.
   */
  public static <T> T valueOrDefault(T value, T defaultValue)
  {
    return value == null ? defaultValue : value;
  }

}
