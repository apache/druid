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

package org.apache.druid.server.lookup.cache;

import org.apache.druid.error.DruidException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LookupLoadingSpecTest
{
  @Test
  public void testLoadingAllLookups()
  {
    LookupLoadingSpec spec = LookupLoadingSpec.ALL;
    Assert.assertEquals(LookupLoadingSpec.Mode.ALL, spec.getMode());
    Assert.assertNull(spec.getLookupsToLoad());
  }

  @Test
  public void testLoadingNoLookups()
  {
    LookupLoadingSpec spec = LookupLoadingSpec.NONE;
    Assert.assertEquals(LookupLoadingSpec.Mode.NONE, spec.getMode());
    Assert.assertEquals(Collections.emptyList(), spec.getLookupsToLoad());
  }

  @Test
  public void testLoadingPartialLookups()
  {
    List<String> lookupsToLoad = Arrays.asList("lookupName1", "lookupName2");
    LookupLoadingSpec spec = LookupLoadingSpec.loadOnly(lookupsToLoad);
    Assert.assertEquals(LookupLoadingSpec.Mode.ONLY_REQUIRED, spec.getMode());
    Assert.assertEquals(lookupsToLoad, spec.getLookupsToLoad());
  }

  @Test
  public void testLoadingPartialLookupsWithNullList()
  {
    DruidException exception = Assert.assertThrows(DruidException.class, () -> LookupLoadingSpec.loadOnly(null));
    Assert.assertEquals("Expected non-null list of lookups to load.", exception.getMessage());
  }
}
