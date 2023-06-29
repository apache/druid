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

package org.apache.druid.msq.indexing;

import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.easymock.EasyMock.mock;

public class MSQPartitionAssignmentTest
{

  @Test(expected = NullPointerException.class)
  public void testNullPartition()
  {
    new MSQPartitionAssignment(null, Collections.emptyMap());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidPartition()
  {
    Map<Integer, SegmentIdWithShardSpec> allocations = ImmutableMap.of(-1, mock(SegmentIdWithShardSpec.class));
    new MSQPartitionAssignment(ClusterByPartitions.oneUniversalPartition(), allocations);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(MSQPartitionAssignment.class)
                  .withNonnullFields("partitions", "allocations")
                  .usingGetClass()
                  .verify();
  }
}
