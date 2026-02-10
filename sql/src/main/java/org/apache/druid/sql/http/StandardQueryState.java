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

package org.apache.druid.sql.http;

/**
 * Standard strings returned by {@link QueryInfo#state()}. Engines can add their own if they like.
 */
public class StandardQueryState
{
  public static final String ACCEPTED = "ACCEPTED";
  public static final String RUNNING = "RUNNING";
  public static final String SUCCESS = "SUCCESS";
  public static final String FAILED = "FAILED";
  public static final String CANCELED = "CANCELED";

  private StandardQueryState()
  {
    // No instantiation.
  }
}
