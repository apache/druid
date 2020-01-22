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

package org.apache.druid.query.extraction;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class ReplaceMissingValueExtractionFnTest extends InitializedNullHandlingTest
{

  @Test
  public void testApply()
  {
    ExtractionFn extractionFn = new ReplaceMissingValueExtractionFn("MISSING");

    Assert.assertEquals("123", extractionFn.apply("123"));
    Assert.assertEquals("MISSING", extractionFn.apply(null));
    if (NullHandling.sqlCompatible()) {
      Assert.assertEquals("", extractionFn.apply(""));
    } else {
      Assert.assertEquals("MISSING", extractionFn.apply(""));
    }
  }

  @Test
  public void testNull()
  {
    ExtractionFn extractionFn = new ReplaceMissingValueExtractionFn(null);
    Assert.assertEquals("123", extractionFn.apply("123"));
    Assert.assertEquals(null, extractionFn.apply(null));

    if (NullHandling.sqlCompatible()) {
      Assert.assertEquals("", extractionFn.apply(""));
    } else {
      Assert.assertEquals(null, extractionFn.apply(""));
    }
  }

  @Test
  public void testGetCacheKey()
  {
    ExtractionFn extractionFn = new ReplaceMissingValueExtractionFn("MISSING");
    ExtractionFn other = new ReplaceMissingValueExtractionFn("MISSING");
    Assert.assertArrayEquals(extractionFn.getCacheKey(), other.getCacheKey());

    other = new ReplaceMissingValueExtractionFn("MISSING2");
    Assert.assertFalse(Arrays.equals(extractionFn.getCacheKey(), other.getCacheKey()));
  }
}
