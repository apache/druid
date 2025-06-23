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

package org.apache.druid.simulate.indexing;

import org.apache.druid.testing.simulate.EmbeddedDruidCluster;
import org.apache.druid.testing.simulate.EmbeddedOverlord;
import org.apache.druid.testing.simulate.junit5.IndexingSimulationTestBase;
import org.junit.jupiter.api.Test;

public class ConcurrentAppendAndReplaceSimTest extends IndexingSimulationTestBase
{
  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.empty().addServer(new EmbeddedOverlord());
  }

  @Test
  public void testStuff()
  {
    // How to verify that append and replace took place?

    // There should be some upgraded segments
    // Do we emit any metric?

    // Easy to verify that compaction took place
  }
}
