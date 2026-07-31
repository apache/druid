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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.utils.RuntimeInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SegmentLoaderConfigTest
{
  @Test
  void testBuilderVirtualStorageFlags()
  {
    // Defaults are false.
    final SegmentLoaderConfig defaults = SegmentLoaderConfig.builder().build();
    Assertions.assertFalse(defaults.isVirtualStorage());
    Assertions.assertFalse(defaults.isVirtualStorageEphemeral());

    // The builder sets both.
    final SegmentLoaderConfig config =
        SegmentLoaderConfig.builder().virtualStorage(true).virtualStorageIsEphemeral(true).build();
    Assertions.assertTrue(config.isVirtualStorage());
    Assertions.assertTrue(config.isVirtualStorageEphemeral());
  }

  @Test
  void testVirtualStorageCoalesceGapBytes() throws Exception
  {
    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    jsonMapper.setInjectableValues(new InjectableValues.Std().addValue(RuntimeInfo.class, new RuntimeInfo()));

    // default
    final SegmentLoaderConfig defaults = jsonMapper.readValue("{}", SegmentLoaderConfig.class);
    Assertions.assertEquals(1024L * 1024L, defaults.getVirtualStorageCoalesceGapBytes());

    // configured
    final SegmentLoaderConfig configured = jsonMapper.readValue(
        "{\"virtualStorageCoalesceGapBytes\": 65536}",
        SegmentLoaderConfig.class
    );
    Assertions.assertEquals(65536L, configured.getVirtualStorageCoalesceGapBytes());
  }

  @Test
  void testVirtualStorageMaxFetchRunBytes() throws Exception
  {
    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    jsonMapper.setInjectableValues(new InjectableValues.Std().addValue(RuntimeInfo.class, new RuntimeInfo()));

    // default
    final SegmentLoaderConfig defaults = jsonMapper.readValue("{}", SegmentLoaderConfig.class);
    Assertions.assertEquals(64L * 1024L * 1024L, defaults.getVirtualStorageMaxFetchRunBytes());

    // configured
    final SegmentLoaderConfig configured = jsonMapper.readValue(
        "{\"virtualStorageMaxFetchRunBytes\": 8388608}",
        SegmentLoaderConfig.class
    );
    Assertions.assertEquals(8388608L, configured.getVirtualStorageMaxFetchRunBytes());
  }

  @Test
  public void testToEphemeralVirtualStorageReturnsCopyAndDoesNotMutateOriginal()
  {
    final SegmentLoaderConfig original = SegmentLoaderConfig.builder().build();
    Assertions.assertFalse(original.isVirtualStorage());

    final SegmentLoaderConfig copy = original.toEphemeralVirtualStorage();

    // The copy is an ephemeral virtual-storage config, while tuning settings are preserved.
    Assertions.assertTrue(copy.isVirtualStorage());
    Assertions.assertTrue(copy.isVirtualStorageEphemeral());
    Assertions.assertEquals(original.getVirtualStorageLoadThreads(), copy.getVirtualStorageLoadThreads());
    Assertions.assertEquals(original.isVirtualStorageUseVirtualThreads(), copy.isVirtualStorageUseVirtualThreads());

    // The original is untouched and the copy is a distinct instance.
    Assertions.assertNotSame(original, copy);
    Assertions.assertFalse(original.isVirtualStorage());
  }

  @Test
  void testEnablingVirtualStorageClearsIncompatibleSettingsButKeepsTuning() throws Exception
  {
    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    jsonMapper.setInjectableValues(new InjectableValues.Std().addValue(RuntimeInfo.class, new RuntimeInfo()));
    final SegmentLoaderConfig node = jsonMapper.readValue(
        "{\"lazyLoadOnStart\": true,"
        + " \"deleteOnRemove\": false,"
        + " \"infoDir\": \"/var/druid/segment-cache/info_dir\","
        + " \"numThreadsToLoadSegmentsIntoPageCacheOnDownload\": 4,"
        + " \"numThreadsToLoadSegmentsIntoPageCacheOnBootstrap\": 2,"
        + " \"virtualStorageMetadataReservationEstimate\": 99999999,"
        + " \"virtualStorageCoalesceGapBytes\": 65536}",
        SegmentLoaderConfig.class
    );

    final SegmentLoaderConfig virtual = node.toEphemeralVirtualStorage();

    // Classic on-disk-cache settings are reset to virtual-storage-safe values...
    Assertions.assertFalse(virtual.isLazyLoadOnStart());
    Assertions.assertTrue(virtual.isDeleteOnRemove());
    Assertions.assertNull(virtual.getInfoDir());
    Assertions.assertEquals(0, virtual.getNumThreadsToLoadSegmentsIntoPageCacheOnDownload());
    Assertions.assertEquals(0, virtual.getNumThreadsToLoadSegmentsIntoPageCacheOnBootstrap());
    // ...while virtual-storage tuning is preserved.
    Assertions.assertTrue(virtual.isVirtualStorage());
    Assertions.assertEquals(99999999L, virtual.getVirtualStorageMetadataReservationEstimate());
    Assertions.assertEquals(65536L, virtual.getVirtualStorageCoalesceGapBytes());

    // The original node config is untouched, and a plain toBuilder copy preserves everything (only the ephemeral
    // virtual-storage derivation sanitizes).
    Assertions.assertTrue(node.isLazyLoadOnStart());
    Assertions.assertEquals(4, node.getNumThreadsToLoadSegmentsIntoPageCacheOnDownload());
    final SegmentLoaderConfig plainCopy = node.toBuilder().build();
    Assertions.assertTrue(plainCopy.isLazyLoadOnStart());
    Assertions.assertFalse(plainCopy.isDeleteOnRemove());
    Assertions.assertEquals(4, plainCopy.getNumThreadsToLoadSegmentsIntoPageCacheOnDownload());
  }
}
