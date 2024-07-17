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

/**
 * This class copies over MSQ context parameters from the MSQ extension. This is required to validate the submitted
 * compaction config at the coordinator. The values used here should be kept in sync with those in
 * {@link org.apache.druid.msq.util.MultiStageQueryContext}
 */
public class ClientMSQContext
{
  public static final String CTX_MAX_NUM_TASKS = "maxNumTasks";
  public static final int DEFAULT_MAX_NUM_TASKS = 2;
  /**
   * Limit to ensure that an MSQ compaction task doesn't take up all task slots in a cluster.
   */
  public static final int MAX_TASK_SLOTS_FOR_MSQ_COMPACTION_TASK = 5;
}
