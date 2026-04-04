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

package org.apache.druid.msq.input;

import com.google.common.base.Preconditions;
import org.apache.druid.msq.exec.DataServerQueryHandler;
import org.apache.druid.msq.input.stage.ReadablePartitions;

import java.util.List;

/**
 * An {@link InputSlice} that has been prepared for reading by an {@link InputSliceReader}. Nothing contained in
 * this class references open resources, so this class is not closeable and does not need to be closed.
 */
public class PhysicalInputSlice
{
  private final ReadablePartitions readablePartitions;
  private final List<LoadableSegment> loadableSegments;
  private final List<DataServerQueryHandler> queryableServers;

  public PhysicalInputSlice(
      final ReadablePartitions readablePartitions,
      final List<LoadableSegment> loadableSegments,
      final List<DataServerQueryHandler> queryableServers
  )
  {
    this.readablePartitions = Preconditions.checkNotNull(readablePartitions, "readablePartitions");
    this.loadableSegments = Preconditions.checkNotNull(loadableSegments, "loadableSegments");
    this.queryableServers = Preconditions.checkNotNull(queryableServers, "queryableServers");
  }

  public ReadablePartitions getReadablePartitions()
  {
    return readablePartitions;
  }

  public List<LoadableSegment> getLoadableSegments()
  {
    return loadableSegments;
  }

  public List<DataServerQueryHandler> getQueryableServers()
  {
    return queryableServers;
  }
}
