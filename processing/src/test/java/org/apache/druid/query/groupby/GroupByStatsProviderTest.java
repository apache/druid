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

package org.apache.druid.query.groupby;

import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.query.groupby.epinephelinae.LimitedTemporaryStorage;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;

public class GroupByStatsProviderTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testMergeBufferCount()
  {
    BlockingPool<ByteBuffer> mergeBufferPool = new DefaultBlockingPool<>(() -> ByteBuffer.allocate(100), 10);

    GroupByStatsProvider statsProvider = new GroupByStatsProvider(mergeBufferPool);
    mergeBufferPool.takeBatch(5);

    Assert.assertEquals(5, statsProvider.getAcquiredMergeBufferCount());
  }

  @Test
  public void testMergeBufferAcquisitionTime()
  {
    GroupByStatsProvider statsProvider = new GroupByStatsProvider(null);

    statsProvider.groupByResourceAcquisitionTimeNs(100);
    statsProvider.groupByResourceAcquisitionTimeNs(300);

    Assert.assertEquals(200, statsProvider.getAndResetGroupByResourceAcquisitionStats());
  }

  @Test
  public void testSpilledBytes() throws IOException
  {
    GroupByStatsProvider statsProvider = new GroupByStatsProvider(null);

    LimitedTemporaryStorage temporaryStorage1 =
        new LimitedTemporaryStorage(temporaryFolder.newFolder(), 1024 * 1024);
    LimitedTemporaryStorage temporaryStorage2 =
        new LimitedTemporaryStorage(temporaryFolder.newFolder(), 1024 * 1024);

    statsProvider.registerTemporaryStorage(temporaryStorage1);
    statsProvider.registerTemporaryStorage(temporaryStorage2);

    LimitedTemporaryStorage.LimitedOutputStream outputStream1 = temporaryStorage1.createFile();
    outputStream1.write(5);
    outputStream1.flush();

    LimitedTemporaryStorage.LimitedOutputStream outputStream2 = temporaryStorage2.createFile();
    outputStream2.write(8);
    outputStream2.flush();

    Assert.assertEquals(2, statsProvider.getSpilledBytes());

    temporaryStorage1.close();
    temporaryStorage2.close();

    Assert.assertEquals(2, statsProvider.getSpilledBytes());
    Assert.assertEquals(0, statsProvider.getSpilledBytes());
  }
}
