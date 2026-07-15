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

import org.junit.Assert;
import org.junit.Test;

public class SegmentLoaderConfigTest
{
  @Test
  public void testSetVirtualStorage()
  {
    final SegmentLoaderConfig config = new SegmentLoaderConfig();

    // Verify default values
    Assert.assertFalse(config.isVirtualStorage());
    Assert.assertFalse(config.isVirtualStorageEphemeral());

    // Set both to true
    config.setVirtualStorage(true).setVirtualStorageIsEphemeral(true);

    // Verify both fields are set
    Assert.assertTrue(config.isVirtualStorage());
    Assert.assertTrue(config.isVirtualStorageEphemeral());
  }

  @Test
  public void testWithVirtualStorageReturnsCopyAndDoesNotMutateOriginal()
  {
    final SegmentLoaderConfig original = new SegmentLoaderConfig();
    Assert.assertFalse(original.isVirtualStorage());

    final SegmentLoaderConfig copy = original.withVirtualStorage(true);

    // The copy has the flag flipped, while other settings are preserved.
    Assert.assertTrue(copy.isVirtualStorage());
    Assert.assertEquals(original.getVirtualStorageLoadThreads(), copy.getVirtualStorageLoadThreads());
    Assert.assertEquals(original.isVirtualStorageUseVirtualThreads(), copy.isVirtualStorageUseVirtualThreads());

    // The original is untouched and the copy is a distinct instance.
    Assert.assertNotSame(original, copy);
    Assert.assertFalse(original.isVirtualStorage());
  }
}
