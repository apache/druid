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

package org.apache.druid.cli;

import com.google.inject.Injector;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.metadata.segment.SqlSegmentsMetadataManagerV2;
import org.apache.druid.metadata.segment.cache.HeapMemorySegmentMetadataCache;
import org.apache.druid.metadata.segment.cache.SegmentMetadataCache;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class CliOverlordTest
{
  @Test
  public void testSegmentMetadataCacheIsBound()
  {
    final Injector injector = new StartupInjectorBuilder().forServer().build();
    final CliOverlord overlord = new CliOverlord();
    injector.injectMembers(overlord);

    final Injector overlordInjector = overlord.makeInjector(Set.of(NodeRole.OVERLORD));

    final SegmentMetadataCache segmentMetadataCache
        = overlordInjector.getInstance(SegmentMetadataCache.class);
    Assert.assertTrue(segmentMetadataCache instanceof HeapMemorySegmentMetadataCache);

    final SegmentsMetadataManager segmentsMetadataManager
        = overlordInjector.getInstance(SegmentsMetadataManager.class);
    Assert.assertTrue(segmentsMetadataManager instanceof SqlSegmentsMetadataManagerV2);
  }


  @Test
  public void testGetDefaultMaxConcurrentActions()
  {
    // Small thread count where
    Assert.assertEquals(8, CliOverlord.getDefaultMaxConcurrentActions(10));
    
    // Medium thread count where
    Assert.assertEquals(21, CliOverlord.getDefaultMaxConcurrentActions(25));
    Assert.assertEquals(26, CliOverlord.getDefaultMaxConcurrentActions(30));


    // Large thread count
    Assert.assertEquals(46, CliOverlord.getDefaultMaxConcurrentActions(50));
    Assert.assertEquals(96, CliOverlord.getDefaultMaxConcurrentActions(100));
    
    // Test edge cases - return atleast 1 thread
    Assert.assertEquals(1, CliOverlord.getDefaultMaxConcurrentActions(-1));
    Assert.assertEquals(1, CliOverlord.getDefaultMaxConcurrentActions(0));

    // Test small clustesr
    Assert.assertEquals(2, CliOverlord.getDefaultMaxConcurrentActions(3));
    Assert.assertEquals(3, CliOverlord.getDefaultMaxConcurrentActions(4));
    Assert.assertEquals(4, CliOverlord.getDefaultMaxConcurrentActions(5));
  }
}
