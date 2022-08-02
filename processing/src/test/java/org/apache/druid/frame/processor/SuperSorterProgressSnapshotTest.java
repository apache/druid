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

package org.apache.druid.frame.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class SuperSorterProgressSnapshotTest
{
  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final SuperSorterProgressSnapshot snapshot = new SuperSorterProgressSnapshot(
        1,
        ImmutableMap.of(1, 2L),
        ImmutableMap.of(1, 3L),
        8,
        false
    );

    final String jsonString = jsonMapper.writeValueAsString(snapshot);
    Assert.assertEquals(snapshot, jsonMapper.readValue(jsonString, SuperSorterProgressSnapshot.class));
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(SuperSorterProgressSnapshot.class).usingGetClass().verify();
  }
}
