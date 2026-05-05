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

package org.apache.druid.timeline.partition;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TombstonePartitionedChunkTest
{

  final TombstoneShardSpec tombstoneShardSpec = new TombstoneShardSpec();
  final TombstonePartitionedChunk tombstonePartitionedChunk =
      TombstonePartitionedChunk.make(tombstoneShardSpec);

  @Test
  public void make()
  {
    Assertions.assertTrue(TombstonePartitionedChunk.make(tombstoneShardSpec) != null);
  }

  @Test
  public void getObject()
  {
    Assertions.assertEquals(tombstoneShardSpec, tombstonePartitionedChunk.getObject());
  }

  @Test
  public void abuts()
  {
    Assertions.assertFalse(tombstonePartitionedChunk.abuts(tombstonePartitionedChunk));
  }

  @Test
  public void isStart()
  {
    Assertions.assertTrue(tombstonePartitionedChunk.isStart());
  }

  @Test
  public void isEnd()
  {
    Assertions.assertTrue(tombstonePartitionedChunk.isEnd());
  }

  @Test
  public void getChunkNumber()
  {
    Assertions.assertEquals(0, tombstonePartitionedChunk.getChunkNumber());
  }

  @Test
  public void compareTo()
  {
    Assertions.assertEquals(0, tombstonePartitionedChunk.compareTo(
        TombstonePartitionedChunk.make(new Object())));
    Exception exception = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> tombstonePartitionedChunk.compareTo(
            new NumberedPartitionChunk<>(0, 1, new Object()))
    );
    Assertions.assertEquals("Cannot compare against something that is not a TombstonePartitionedChunk.",
                        exception.getMessage());
  }

  @Test
  public void equalsTest()
  {
    TombstonePartitionedChunk aCopy = tombstonePartitionedChunk;
    Assertions.assertTrue(tombstonePartitionedChunk.equals(aCopy));
    Assertions.assertFalse(tombstonePartitionedChunk.equals(null));
    Assertions.assertFalse(tombstonePartitionedChunk.equals(new Object()));
    Assertions.assertTrue(tombstonePartitionedChunk.equals(TombstonePartitionedChunk.make(new Object())));
  }

  // make jacoco happy:
  @Test
  public void minutia()
  {
    Assertions.assertTrue(tombstonePartitionedChunk.hashCode() > 0);
    Assertions.assertNotNull(tombstonePartitionedChunk.toString());
  }

}
