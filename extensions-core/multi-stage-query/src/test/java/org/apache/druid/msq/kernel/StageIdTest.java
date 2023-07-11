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

package org.apache.druid.msq.kernel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class StageIdTest
{
  @Test
  public void testCompareTo()
  {
    final List<StageId> sortedStageIds = Ordering.natural().sortedCopy(
        ImmutableList.of(
            new StageId("xyz", 3),
            new StageId("xyz", 1),
            new StageId("xyz", 2),
            new StageId("abc", 2),
            new StageId("abc", 1)
        )
    );

    Assert.assertEquals(
        ImmutableList.of(
            new StageId("abc", 1),
            new StageId("abc", 2),
            new StageId("xyz", 1),
            new StageId("xyz", 2),
            new StageId("xyz", 3)
        ),
        sortedStageIds
    );
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper();
    final StageId stageId = new StageId("abc", 1);

    Assert.assertEquals(
        stageId,
        mapper.readValue(mapper.writeValueAsString(stageId), StageId.class)
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(StageId.class).usingGetClass().verify();
  }
}
