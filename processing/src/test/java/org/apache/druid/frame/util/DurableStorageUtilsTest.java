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

package org.apache.druid.frame.util;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

public class DurableStorageUtilsTest
{

  @Test
  public void getNextDirNameWithPrefixFromPath()
  {
    Assert.assertEquals("", DurableStorageUtils.getNextDirNameWithPrefixFromPath("/123/123"));
    Assert.assertEquals("123", DurableStorageUtils.getNextDirNameWithPrefixFromPath("123"));
    Assert.assertEquals("controller_query_123",
                        DurableStorageUtils.getNextDirNameWithPrefixFromPath("controller_query_123/123"));
    Assert.assertEquals("", DurableStorageUtils.getNextDirNameWithPrefixFromPath(""));
    Assert.assertNull(DurableStorageUtils.getNextDirNameWithPrefixFromPath(null));
  }

  @Test
  public void isQueryResultFileActive()
  {

    Assert.assertTrue(DurableStorageUtils.isQueryResultFileActive(
        DurableStorageUtils.QUERY_RESULTS_DIR + "/123/result",
        ImmutableSet.of("123")
    ));
    Assert.assertFalse(DurableStorageUtils.isQueryResultFileActive(
        DurableStorageUtils.QUERY_RESULTS_DIR + "/123/result",
        ImmutableSet.of("")
    ));
    Assert.assertFalse(DurableStorageUtils.isQueryResultFileActive(
        DurableStorageUtils.QUERY_RESULTS_DIR + "/",
        ImmutableSet.of("123")
    ));
    Assert.assertFalse(DurableStorageUtils.isQueryResultFileActive(
        null,
        ImmutableSet.of("123")
    ));
    Assert.assertFalse(DurableStorageUtils.isQueryResultFileActive(
        DurableStorageUtils.QUERY_RESULTS_DIR,
        ImmutableSet.of("123")
    ));
  }
}
