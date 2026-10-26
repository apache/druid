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

package org.apache.druid.client.indexing;

import org.apache.druid.query.context.QueryContextParameters;

/**
 * MSQ values used to validate the submitted compaction config at the coordinator. MSQ query context parameters, such
 * as {@link org.apache.druid.query.context.QueryContextParameters#MAX_NUM_TASKS}, are declared in
 * {@link org.apache.druid.query.context.QueryContextParameters}.
 */
public class ClientMSQContext
{
  /**
   * @deprecated Use {@link QueryContextParameters#MAX_NUM_TASKS} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_MAX_NUM_TASKS = "maxNumTasks";
  /**
   * @deprecated Use the default value of {@link QueryContextParameters#MAX_NUM_TASKS} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final int DEFAULT_MAX_NUM_TASKS = 2;
  /**
   * Limit to ensure that an MSQ compaction task doesn't take up all task slots in a cluster.
   */
  public static final int MAX_TASK_SLOTS_FOR_MSQ_COMPACTION_TASK = 5;
}
