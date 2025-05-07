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
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.metadata.segment.SqlSegmentsMetadataManagerV2;
import org.apache.druid.metadata.segment.cache.HeapMemorySegmentMetadataCache;
import org.apache.druid.metadata.segment.cache.SegmentMetadataCache;
import org.junit.Assert;
import org.junit.Test;

public class CliOverlordTest
{
  @Test
  public void testSegmentMetadataCacheIsBound()
  {
    final Injector injector = new StartupInjectorBuilder().forServer().build();
    final CliOverlord overlord = new CliOverlord();
    injector.injectMembers(overlord);

    final Injector overlordInjector = overlord.makeInjector();

    final SegmentMetadataCache segmentMetadataCache
        = overlordInjector.getInstance(SegmentMetadataCache.class);
    Assert.assertTrue(segmentMetadataCache instanceof HeapMemorySegmentMetadataCache);

    final SegmentsMetadataManager segmentsMetadataManager
        = overlordInjector.getInstance(SegmentsMetadataManager.class);
    Assert.assertTrue(segmentsMetadataManager instanceof SqlSegmentsMetadataManagerV2);
  }
}
