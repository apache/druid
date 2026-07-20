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

import org.apache.druid.query.extraction.RegexDimExtractionFn;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class RegexDimFilterTest
{

  @Test
  public void testGetCacheKey()
  {
    RegexDimFilter regexDimFilter = new RegexDimFilter("dim", "reg", null);
    RegexDimFilter regexDimFilter2 = new RegexDimFilter("di", "mreg", null);
    Assertions.assertFalse(Arrays.equals(regexDimFilter.getCacheKey(), regexDimFilter2.getCacheKey()));

    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    RegexDimFilter regexDimFilter3 = new RegexDimFilter("dim", "reg", regexFn);
    Assertions.assertFalse(Arrays.equals(regexDimFilter.getCacheKey(), regexDimFilter3.getCacheKey()));

  }

  @Test
  public void testEquals()
  {
    RegexDimFilter regexDimFilter = new RegexDimFilter("dim", "reg", null);
    RegexDimFilter regexDimFilter2 = new RegexDimFilter("di", "mreg", null);
    RegexDimFilter regexDimFilter3 = new RegexDimFilter("di", "mreg", null);

    Assertions.assertNotEquals(regexDimFilter, regexDimFilter2);
    Assertions.assertEquals(regexDimFilter2, regexDimFilter3);

    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    RegexDimFilter regexDimFilter4 = new RegexDimFilter("dim", "reg", regexFn);
    RegexDimFilter regexDimFilter5 = new RegexDimFilter("dim", "reg", regexFn);
    Assertions.assertNotEquals(regexDimFilter, regexDimFilter4);
    Assertions.assertEquals(regexDimFilter4, regexDimFilter5);

  }

  @Test
  public void testHashcode()
  {
    RegexDimFilter regexDimFilter = new RegexDimFilter("dim", "reg", null);
    RegexDimFilter regexDimFilter2 = new RegexDimFilter("di", "mreg", null);
    RegexDimFilter regexDimFilter3 = new RegexDimFilter("di", "mreg", null);

    Assertions.assertNotEquals(regexDimFilter.hashCode(), regexDimFilter2.hashCode());
    Assertions.assertEquals(regexDimFilter2.hashCode(), regexDimFilter3.hashCode());

    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    RegexDimFilter regexDimFilter4 = new RegexDimFilter("dim", "reg", regexFn);
    RegexDimFilter regexDimFilter5 = new RegexDimFilter("dim", "reg", regexFn);
    Assertions.assertNotEquals(regexDimFilter.hashCode(), regexDimFilter4.hashCode());
    Assertions.assertEquals(regexDimFilter4.hashCode(), regexDimFilter5.hashCode());
  }

}
