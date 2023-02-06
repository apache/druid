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

package org.apache.druid.msq.indexing.error;

/**
 * Helper class for defining parameters used by the multi-stage query engine's "warning framework"
 */
public class MSQWarnings
{
  /**
   * Query context key for limiting the maximum number of parse exceptions that a multi-stage query can generate
   */
  public static final String CTX_MAX_PARSE_EXCEPTIONS_ALLOWED = "maxParseExceptions";

  /**
   * Default number of parse exceptions permissible for a multi-stage query
   */
  public static final Long DEFAULT_MAX_PARSE_EXCEPTIONS_ALLOWED = -1L;
}
