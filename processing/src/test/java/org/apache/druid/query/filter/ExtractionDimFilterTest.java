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

package org.apache.druid.query.filter;

import org.apache.druid.query.extraction.IdentityExtractionFn;
import org.apache.druid.query.extraction.RegexDimExtractionFn;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class ExtractionDimFilterTest
{

  @Test
  public void testGetCacheKey()
  {
    ExtractionDimFilter extractionDimFilter = new ExtractionDimFilter(
        "abc",
        "d",
        IdentityExtractionFn.getInstance(),
        null
    );
    ExtractionDimFilter extractionDimFilter2 = new ExtractionDimFilter(
        "ab",
        "cd",
        IdentityExtractionFn.getInstance(),
        null
    );

    Assert.assertFalse(Arrays.equals(extractionDimFilter.getCacheKey(), extractionDimFilter2.getCacheKey()));

    ExtractionDimFilter extractionDimFilter3 = new ExtractionDimFilter(
        "ab",
        "cd",
        new RegexDimExtractionFn("xx", null, null),
        null
    );

    Assert.assertFalse(Arrays.equals(extractionDimFilter2.getCacheKey(), extractionDimFilter3.getCacheKey()));

    Assert.assertNotNull(new ExtractionDimFilter("foo", null, new RegexDimExtractionFn("xx", null, null), null).getCacheKey());
  }
}
