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

package org.apache.druid.guice;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.OmniDataSegmentKiller;
import org.apache.druid.segment.loading.RandomStorageLocationSelectorStrategy;
import org.apache.druid.segment.loading.StorageLocation;
import org.apache.druid.segment.loading.StorageLocationSelectorStrategy;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class LocalDataStorageDruidModuleTest
{
  @Test
  public void testSegmentKillerBoundSingleton()
  {
    Injector injector = createInjector();
    OmniDataSegmentKiller killer = injector.getInstance(OmniDataSegmentKiller.class);
    Assertions.assertTrue(killer.getKillers().containsKey(LocalDataStorageDruidModule.SCHEME));
    Assertions.assertSame(
        killer.getKillers().get(LocalDataStorageDruidModule.SCHEME).get(),
        killer.getKillers().get(LocalDataStorageDruidModule.SCHEME).get()
    );
  }

  /**
   * {@code druid.storage.zip} is bound by this module for every deep storage implementation to read, so check that it
   * actually reaches the pusher the module provisions, rather than only that it parses.
   */
  @Test
  public void testDataSegmentPusherHonorsStorageZip(@TempDir File tempDir) throws IOException
  {
    Assertions.assertTrue(new File(pushSegment(tempDir, "true"), "index.zip").isFile());
  }

  @Test
  public void testDataSegmentPusherDefaultsToUnzipped(@TempDir File tempDir) throws IOException
  {
    // local deep storage writes a directory of files when the property is unset, which is the default the tri-state
    // DeepStorageSegmentConfig exists to preserve
    Assertions.assertTrue(new File(pushSegment(tempDir, null), "index").isDirectory());
  }

  /**
   * Pushes a one-file segment through a {@link DataSegmentPusher} provisioned by this module, and returns the
   * directory it was pushed to.
   */
  private static File pushSegment(File tempDir, @Nullable String zip) throws IOException
  {
    final File storageDir = new File(tempDir, "deepStorage");
    final File segmentDir = new File(tempDir, "segment");
    FileUtils.mkdirp(segmentDir);
    Files.asByteSink(new File(segmentDir, "version.bin")).write(Ints.toByteArray(0x9));

    final Injector injector = createInjector();
    // JsonConfigProvider reads these lazily, on the first getInstance below
    final Properties properties = injector.getInstance(Properties.class);
    properties.setProperty("druid.storage.storageDirectory", storageDir.getAbsolutePath());
    if (zip != null) {
      properties.setProperty("druid.storage.zip", zip);
    }

    final DataSegmentPusher pusher = injector.getInstance(DataSegmentPusher.class);
    final DataSegment segment = DataSegment.builder(SegmentId.of("ds", Intervals.utc(0, 1), "v1", 0)).build();

    return new File(storageDir, pusher.getStorageDir(pusher.push(segmentDir, segment, false), false));
  }

  private static Injector createInjector()
  {
    return GuiceInjectors.makeStartupInjectorWithModules(
        ImmutableList.of(
            new LocalDataStorageDruidModule(),
            (Module) binder -> {
              binder.bind(new TypeLiteral<List<StorageLocation>>(){}).toInstance(ImmutableList.of());
              binder.bind(ColumnConfig.class).toInstance(ColumnConfig.DEFAULT);
              binder.bind(StorageLocationSelectorStrategy.class)
                    .toInstance(new RandomStorageLocationSelectorStrategy(ImmutableList.of()));
            }
        )
    );
  }
}
