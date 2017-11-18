/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.overlord.supervisor;

import io.druid.indexing.overlord.DataSourceMetadata;

import javax.annotation.Nullable;

public interface Supervisor
{
  void start();

  /**
   * @param stopGracefully If true, supervisor will cleanly shutdown managed tasks if possible (for example signalling
   *                       them to publish their segments and exit). The implementation may block until the tasks have
   *                       either acknowledged or completed. If false, supervisor will stop immediately and leave any
   *                       running tasks as they are.
   */
  void stop(boolean stopGracefully);

  SupervisorReport getStatus();

  void reset(DataSourceMetadata dataSourceMetadata);

  /**
   * The definition of checkpoint is not very strict as currently it does not affect data or control path
   * On this call Supervisor can potentially checkpoint data processed so far to some durable storage
   * for example - Kafka Supervisor uses this to merge and handoff segments containing at least the data
   * represented by dataSourceMetadata
   *
   * @param sequenceName       unique Identifier to figure out for which sequence to do check pointing
   * @param previousCheckPoint DataSourceMetadata check pointed in previous call
   * @param currentCheckPoint  current DataSourceMetadata to be check pointed
   */
  void checkpoint(
      @Nullable String sequenceName,
      @Nullable DataSourceMetadata previousCheckPoint,
      @Nullable DataSourceMetadata currentCheckPoint
  );
}
