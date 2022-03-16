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

import org.junit.Assert;
import org.junit.Test;

public class TombstonePartitionedChunkTest
{

  final TombstoneShardSpec tombstoneShardSpec = new TombstoneShardSpec();
  final TombstonePartitionedChunk tombstonePartitionedChunk =
      TombstonePartitionedChunk.make(tombstoneShardSpec);

  @Test
  public void make()
  {
    Assert.assertTrue(TombstonePartitionedChunk.make(tombstoneShardSpec) != null);
  }

  @Test
  public void getObject()
  {
    Assert.assertEquals(tombstoneShardSpec, tombstonePartitionedChunk.getObject());
  }

  @Test
  public void abuts()
  {
    Assert.assertFalse(tombstonePartitionedChunk.abuts(tombstonePartitionedChunk));
  }

  @Test
  public void isStart()
  {
    Assert.assertTrue(tombstonePartitionedChunk.isStart());
  }

  @Test
  public void isEnd()
  {
    Assert.assertTrue(tombstonePartitionedChunk.isEnd());
  }

  @Test
  public void getChunkNumber()
  {
    Assert.assertEquals(0, tombstonePartitionedChunk.getChunkNumber());
  }

  @Test
  public void compareTo()
  {
    Assert.assertEquals(0, tombstonePartitionedChunk.compareTo(
        TombstonePartitionedChunk.make(new Object())));
    Exception exception = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> tombstonePartitionedChunk.compareTo(
              new NumberedPartitionChunk<Object>(0, 1, new Object()))
    );
    Assert.assertEquals("Cannot compare against something that is not a TombstonePartitionedChunk.",
                        exception.getMessage());
  }

  @Test
  public void equalsTest()
  {
    TombstonePartitionedChunk aCopy = tombstonePartitionedChunk;
    Assert.assertTrue(tombstonePartitionedChunk.equals(aCopy));
    Assert.assertFalse(tombstonePartitionedChunk.equals(null));
    Assert.assertFalse(tombstonePartitionedChunk.equals(new Object()));
    Assert.assertTrue(tombstonePartitionedChunk.equals(TombstonePartitionedChunk.make(new Object())));
  }

  // make jacoco happy:
  @Test
  public void minutia()
  {
    Assert.assertTrue(tombstonePartitionedChunk.hashCode() > 0);
    Assert.assertNotNull(tombstonePartitionedChunk.toString());
  }

}
