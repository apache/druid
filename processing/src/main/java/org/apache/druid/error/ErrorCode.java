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

package org.apache.druid.error;

public class ErrorCode
{
  public static final String SQL_GROUP = "SQL";
  public static final String INTERNAL_GROUP = "INTERNAL";

  public static final String SQL_VALIDATION_GROUP = SQL_GROUP + "-Validation";
  public static final String SQL_PARSE_GROUP = SQL_GROUP + "-Parse";
  public static final String SQL_UNSUPPORTED_GROUP = SQL_GROUP + "-Unsupported";

  public static final String GENERAL_TAIL = "General";

  public static String fullCode(String base, String tail)
  {
    return base + "-" + tail;
  }
}
