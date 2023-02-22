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

package org.apache.druid.msq.input.stage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class CombinedReadablePartitionsTest
{
  private static final CombinedReadablePartitions PARTITIONS = ReadablePartitions.combine(
      ImmutableList.of(
          ReadablePartitions.striped(0, 2, 2),
          ReadablePartitions.striped(1, 2, 4)
      )
  );

  @Test
  public void testEmpty()
  {
    Assert.assertEquals(0, Iterables.size(ReadablePartitions.empty()));
  }

  @Test
  public void testSplitOne()
  {
    Assert.assertEquals(ImmutableList.of(PARTITIONS), PARTITIONS.split(1));
  }

  @Test
  public void testSplitTwo()
  {
    Assert.assertEquals(
        ImmutableList.of(
            ReadablePartitions.combine(
                ImmutableList.of(
                    new StripedReadablePartitions(0, 2, new IntAVLTreeSet(new int[]{0})),
                    new StripedReadablePartitions(1, 2, new IntAVLTreeSet(new int[]{0, 2}))
                )
            ),
            ReadablePartitions.combine(
                ImmutableList.of(
                    new StripedReadablePartitions(0, 2, new IntAVLTreeSet(new int[]{1})),
                    new StripedReadablePartitions(1, 2, new IntAVLTreeSet(new int[]{1, 3}))
                )
            )
        ),
        PARTITIONS.split(2)
    );
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper()
                                          .registerModules(new MSQIndexingModule().getJacksonModules());

    Assert.assertEquals(
        PARTITIONS,
        mapper.readValue(
            mapper.writeValueAsString(PARTITIONS),
            ReadablePartitions.class
        )
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(CombinedReadablePartitions.class).usingGetClass().verify();
  }
}
