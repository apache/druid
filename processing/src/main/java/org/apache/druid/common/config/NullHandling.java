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

import com.google.inject.Inject;

/**
 * Some dead constants
 */
public class NullHandling
{
  public static final byte IS_NULL_BYTE = (byte) 1;
  public static final byte IS_NOT_NULL_BYTE = (byte) 0;

  /**
   * INSTANCE is injected using static injection to avoid adding JacksonInject annotations all over the code.
   * See org.apache.druid.guice.NullHandlingModule for details.
   * It does not take effect in all unit tests since we don't use Guice Injection.
   */
  @Inject
  private static NullValueHandlingConfig INSTANCE;

}
