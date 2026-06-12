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

package org.apache.druid.segment.handoff;


public interface SegmentHandoffNotifierFactory
{
  SegmentHandoffNotifier createSegmentHandoffNotifier(String dataSource, String taskId);

  default SegmentHandoffNotifier createSegmentHandoffNotifier(
      final String dataSource,
      final String taskId,
      final boolean strictTierAwareSegmentLoad
  )
  {
    if (strictTierAwareSegmentLoad) {
      throw new UnsupportedOperationException(
          "Strict tier-aware segment load handoff checks are not supported by this "
          + "SegmentHandoffNotifierFactory implementation."
      );
    }
    return createSegmentHandoffNotifier(dataSource, taskId);
  }
}
