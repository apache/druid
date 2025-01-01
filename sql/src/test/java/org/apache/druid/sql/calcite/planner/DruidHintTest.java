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

package org.apache.druid.sql.calcite.planner;

import org.apache.druid.query.JoinAlgorithm;
import org.junit.Assert;
import org.junit.Test;

public class DruidHintTest
{
  @Test
  public void testFromString()
  {
    Assert.assertEquals(DruidHint.DruidJoinHint.fromString("sort_merge").id(), DruidHint.DruidJoinHint.SortMergeJoinHint.SORT_MERGE_JOIN);
    Assert.assertEquals(DruidHint.DruidJoinHint.fromString("broadcast").id(), DruidHint.DruidJoinHint.BroadcastJoinHint.BROADCAST_JOIN);
    Assert.assertNull(DruidHint.DruidJoinHint.fromString("hash"));

    Assert.assertEquals(DruidHint.DruidJoinHint.fromString("sort_merge").asJoinAlgorithm(), JoinAlgorithm.SORT_MERGE);
    Assert.assertEquals(DruidHint.DruidJoinHint.fromString("broadcast").asJoinAlgorithm(), JoinAlgorithm.BROADCAST);
  }
}
