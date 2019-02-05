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

package org.apache.druid.query.lookup;

import com.google.common.collect.ImmutableMap;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LookupIntrospectionResourceImplTest
{

  LookupReferencesManager lookupReferencesManager = EasyMock.createMock(LookupReferencesManager.class);
  LookupIntrospectionResource lookupIntrospectionResource = new LookupIntrospectionResource(lookupReferencesManager);

  @Before
  public void setUp() throws Exception
  {
    EasyMock.reset(lookupReferencesManager);
    LookupExtractorFactory lookupExtractorFactory1 = new MapLookupExtractorFactory(ImmutableMap.of(
        "key",
        "value",
        "key2",
        "value2"
    ), false);
    EasyMock.expect(lookupReferencesManager.get("lookupId1")).andReturn(
        new LookupExtractorFactoryContainer(
            "v0",
            lookupExtractorFactory1
        )
    ).anyTimes();
    EasyMock.replay(lookupReferencesManager);
  }

  @Test
  public void testGetKey()
  {
    MapLookupExtractorFactory.MapLookupIntrospectionHandler mapLookupIntrospectionHandler =
        (MapLookupExtractorFactory.MapLookupIntrospectionHandler) lookupIntrospectionResource
            .introspectLookup("lookupId1");
    Assert.assertEquals("[key, key2]", mapLookupIntrospectionHandler.getKeys().getEntity());
  }

  @Test
  public void testGetValue()
  {
    MapLookupExtractorFactory.MapLookupIntrospectionHandler mapLookupIntrospectionHandler =
        (MapLookupExtractorFactory.MapLookupIntrospectionHandler) lookupIntrospectionResource
            .introspectLookup("lookupId1");
    Assert.assertEquals("[value, value2]", mapLookupIntrospectionHandler.getValues().getEntity());
  }

  @Test
  public void testGetMap()
  {
    MapLookupExtractorFactory.MapLookupIntrospectionHandler mapLookupIntrospectionHandler =
        (MapLookupExtractorFactory.MapLookupIntrospectionHandler) lookupIntrospectionResource
            .introspectLookup("lookupId1");
    Assert.assertEquals(
        ImmutableMap.of("key", "value", "key2", "value2"),
        mapLookupIntrospectionHandler.getMap().getEntity()
    );
  }
}
