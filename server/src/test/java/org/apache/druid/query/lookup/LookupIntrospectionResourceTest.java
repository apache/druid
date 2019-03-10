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
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;

/**
 * Test of actual introspection functionality available in {@link LookupIntrospectionResourceImplTest}
 */
public class LookupIntrospectionResourceTest
{
  private LookupExtractorFactoryContainerProvider lookupExtractorFactoryContainerProvider =
      EasyMock.createMock(LookupExtractorFactoryContainerProvider.class);
  private LookupExtractorFactory lookupExtractorFactory = EasyMock.createMock(LookupExtractorFactory.class);
  private LookupIntrospectHandler lookupIntrospectHandler = EasyMock.createMock(LookupIntrospectHandler.class);

  private LookupIntrospectionResource lookupIntrospectionResource = new LookupIntrospectionResource(
      lookupExtractorFactoryContainerProvider);

  @Before
  public void setUp()
  {
    EasyMock.expect(lookupExtractorFactoryContainerProvider.get("lookupId")).andReturn(
        new LookupExtractorFactoryContainer(
            "v0",
            lookupExtractorFactory
        )
    ).anyTimes();
    EasyMock.expect(lookupExtractorFactoryContainerProvider.get(EasyMock.anyString())).andReturn(null).anyTimes();
    EasyMock.replay(lookupExtractorFactoryContainerProvider);
  }

  @Test
  public void testNotImplementedIntrospectLookup()
  {
    EasyMock.expect(lookupExtractorFactory.getIntrospectHandler()).andReturn(null);
    EasyMock.expect(lookupExtractorFactory.get())
            .andReturn(new MapLookupExtractor(ImmutableMap.of(), false))
            .anyTimes();
    EasyMock.replay(lookupExtractorFactory);
    Assert.assertEquals(
        Response.status(Response.Status.NOT_FOUND).build().getStatus(),
        ((Response) lookupIntrospectionResource.introspectLookup("lookupId")).getStatus()
    );
  }

  @Test
  public void testNotExistingLookup()
  {
    Assert.assertEquals(
        Response.status(Response.Status.NOT_FOUND).build().getStatus(),
        ((Response) lookupIntrospectionResource.introspectLookup("not there")).getStatus()
    );
  }

  @Test public void testExistingLookup()
  {
    EasyMock.expect(lookupExtractorFactory.getIntrospectHandler()).andReturn(lookupIntrospectHandler);
    EasyMock.expect(lookupExtractorFactory.get())
            .andReturn(new MapLookupExtractor(ImmutableMap.of(), false))
            .anyTimes();
    EasyMock.replay(lookupExtractorFactory);
    Assert.assertEquals(lookupIntrospectHandler, lookupIntrospectionResource.introspectLookup("lookupId"));
  }
}
