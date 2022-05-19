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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.common.config.NullHandling;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class UpperExtractionFnTest
{
  ExtractionFn extractionFn = new UpperExtractionFn(null);

  @Test
  public void testApply()
  {
    Assert.assertEquals("UPPER", extractionFn.apply("uPpeR"));
    Assert.assertEquals(NullHandling.replaceWithDefault() ? null : "", extractionFn.apply(""));
    Assert.assertEquals(null, extractionFn.apply(null));
    Assert.assertEquals(null, extractionFn.apply((Object) null));
    Assert.assertEquals("1", extractionFn.apply(1));
  }

  @Test
  public void testGetCacheKey()
  {
    Assert.assertArrayEquals(extractionFn.getCacheKey(), extractionFn.getCacheKey());
    Assert.assertFalse(Arrays.equals(extractionFn.getCacheKey(), new LowerExtractionFn(null).getCacheKey()));
  }

  @Test
  public void testEqualsContract()
  {
    EqualsVerifier.forClass(UpperExtractionFn.class)
                  .usingGetClass()
                  .verify();
  }
}
