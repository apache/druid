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

import com.google.common.collect.ImmutableSet;
import org.apache.druid.error.DruidException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

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
    Assert.assertNull(spec.getLookupsToLoad());
  }

  @Test
  public void testLoadingOnlyRequiredLookups()
  {
    Set<String> lookupsToLoad = ImmutableSet.of("lookupName1", "lookupName2");
    LookupLoadingSpec spec = LookupLoadingSpec.loadOnly(lookupsToLoad);
    Assert.assertEquals(LookupLoadingSpec.Mode.ONLY_REQUIRED, spec.getMode());
    Assert.assertEquals(lookupsToLoad, spec.getLookupsToLoad());
  }

  @Test
  public void testLoadingOnlyRequiredLookupsWithNullList()
  {
    DruidException exception = Assert.assertThrows(DruidException.class, () -> LookupLoadingSpec.loadOnly(null));
    Assert.assertEquals("Expected non-null set of lookups to load.", exception.getMessage());
  }
}
