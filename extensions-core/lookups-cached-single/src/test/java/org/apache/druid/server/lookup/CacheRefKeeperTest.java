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

package org.apache.druid.server.lookup;

import org.apache.druid.server.lookup.cache.polling.PollingCache;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

public class CacheRefKeeperTest
{

  @Test
  public void testGet()
  {
    PollingCache mockPollingCache = EasyMock.createStrictMock(PollingCache.class);
    PollingLookup.CacheRefKeeper cacheRefKeeper = new PollingLookup.CacheRefKeeper(mockPollingCache);
    Assert.assertEquals(mockPollingCache, cacheRefKeeper.getAndIncrementRef());
  }

  @Test
  public void testDoneWithIt()
  {
    PollingCache mockPollingCache = EasyMock.createStrictMock(PollingCache.class);
    PollingLookup.CacheRefKeeper cacheRefKeeper = new PollingLookup.CacheRefKeeper(mockPollingCache);
    mockPollingCache.close();
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.replay(mockPollingCache);
    cacheRefKeeper.doneWithIt();
    EasyMock.verify(mockPollingCache);
  }

  @Test
  public void testGetAfterDoneWithIt()
  {
    PollingCache mockPollingCache = EasyMock.createStrictMock(PollingCache.class);
    PollingLookup.CacheRefKeeper cacheRefKeeper = new PollingLookup.CacheRefKeeper(mockPollingCache);
    mockPollingCache.close();
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.replay(mockPollingCache);
    cacheRefKeeper.doneWithIt();
    EasyMock.verify(mockPollingCache);
    Assert.assertEquals(null, cacheRefKeeper.getAndIncrementRef());
    Assert.assertEquals(null, cacheRefKeeper.getAndIncrementRef());
  }

}
