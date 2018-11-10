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

package org.apache.druid.segment.realtime.plumber;

import com.google.inject.Inject;
import org.apache.druid.client.coordinator.CoordinatorClient;

public class CoordinatorBasedSegmentHandoffNotifierFactory implements SegmentHandoffNotifierFactory
{

  private final CoordinatorClient client;
  private final CoordinatorBasedSegmentHandoffNotifierConfig config;

  @Inject
  public CoordinatorBasedSegmentHandoffNotifierFactory(
      CoordinatorClient client,
      CoordinatorBasedSegmentHandoffNotifierConfig config
  )
  {
    this.client = client;
    this.config = config;
  }

  @Override
  public SegmentHandoffNotifier createSegmentHandoffNotifier(String dataSource)
  {
    return new CoordinatorBasedSegmentHandoffNotifier(
        dataSource,
        client,
        config
    );
  }
}
