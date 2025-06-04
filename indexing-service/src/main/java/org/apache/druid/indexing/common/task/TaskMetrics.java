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

package org.apache.druid.indexing.common.task;

/**
 * Task-related metrics emitted by the Druid cluster.
 */
public class TaskMetrics
{
  private TaskMetrics()
  {
    // no instantiation
  }

  public static final String RUN_DURATION = "task/run/time";

  public static final String SEGMENTS_DELETED_FROM_METADATA_STORE = "segment/killed/metadataStore/count";
  public static final String SEGMENTS_DELETED_FROM_DEEPSTORE = "segment/killed/deepStorage/count";
  public static final String FILES_DELETED_FROM_DEEPSTORE = "segment/killed/deepStorageFile/count";
}
