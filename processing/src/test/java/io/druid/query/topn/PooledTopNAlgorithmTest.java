/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.topn;

import io.druid.collections.ResourceHolder;
import io.druid.segment.StorageAdapter;
import org.easymock.EasyMock;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class PooledTopNAlgorithmTest
{
  @Test
  public void testCleanupWithNullParams()
  {
    PooledTopNAlgorithm pooledTopNAlgorithm = new PooledTopNAlgorithm(EasyMock.mock(StorageAdapter.class), null, null);
    pooledTopNAlgorithm.cleanup(null);
  }

  @Test
  public void cleanup() throws IOException
  {
    PooledTopNAlgorithm pooledTopNAlgorithm = new PooledTopNAlgorithm(EasyMock.mock(StorageAdapter.class), null, null);
    PooledTopNAlgorithm.PooledTopNParams params = EasyMock.createMock(PooledTopNAlgorithm.PooledTopNParams.class);
    ResourceHolder<ByteBuffer> resourceHolder = EasyMock.createMock(ResourceHolder.class);
    EasyMock.expect(params.getResultsBufHolder()).andReturn(resourceHolder).times(1);
    EasyMock.expect(resourceHolder.get()).andReturn(ByteBuffer.allocate(1)).times(1);
    resourceHolder.close();
    EasyMock.expectLastCall().once();
    EasyMock.replay(params);
    EasyMock.replay(resourceHolder);
    pooledTopNAlgorithm.cleanup(params);
    EasyMock.verify(params);
    EasyMock.verify(resourceHolder);
  }
}
