/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.lookup;

import com.google.common.collect.ImmutableMap;
import io.druid.query.extraction.MapLookupExtractor;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.core.Response;
import java.io.InputStream;

public class LookupIntrospectionResourceTest
{

  LookupReferencesManager lookupReferencesManager = EasyMock.createMock(LookupReferencesManager.class);
  LookupExtractorFactory lookupExtractorFactory = EasyMock.createMock(LookupExtractorFactory.class);
  LookupIntrospectHandler lookupIntrospectHandler = EasyMock.createMock(LookupIntrospectHandler.class);

  LookupIntrospectionResource lookupIntrospectionResource = new LookupIntrospectionResource(lookupReferencesManager);

  @Before
  public void setUp()
  {
    EasyMock.expect(lookupReferencesManager.get("lookupId")).andReturn(
        new LookupExtractorFactoryContainer(
            "v0",
            lookupExtractorFactory
        )
    ).anyTimes();
    EasyMock.expect(lookupReferencesManager.get(EasyMock.anyString())).andReturn(null).anyTimes();
    EasyMock.replay(lookupReferencesManager);
  }
  @Test
  public void testNotImplementedIntrospectLookup()
  {
    EasyMock.expect(lookupExtractorFactory.getIntrospectHandler()).andReturn(null);
    EasyMock.expect(lookupExtractorFactory.get()).andReturn(new MapLookupExtractor(ImmutableMap.<String, String>of(), false)).anyTimes();
    EasyMock.replay(lookupExtractorFactory);
    Assert.assertEquals(Response.status(Response.Status.NOT_FOUND).build().getStatus(), ((Response) lookupIntrospectionResource.introspectLookup("lookupId")).getStatus());
  }


  @Test
  public void testNotExistingLookup()
  {
    Assert.assertEquals(Response.status(Response.Status.NOT_FOUND).build().getStatus(), ((Response) lookupIntrospectionResource.introspectLookup("not there")).getStatus());
  }

  @Test public void testExistingLookup()
  {
    EasyMock.expect(lookupExtractorFactory.getIntrospectHandler()).andReturn(lookupIntrospectHandler);
    EasyMock.expect(lookupExtractorFactory.get()).andReturn(new MapLookupExtractor(ImmutableMap.<String, String>of(), false)).anyTimes();
    EasyMock.replay(lookupExtractorFactory);
    Assert.assertEquals(lookupIntrospectHandler, lookupIntrospectionResource.introspectLookup("lookupId"));
  }
  @Test
  @Ignore
  public void testIntrospection()
  {

    LookupIntrospectHandler lookupIntrospectHandler = new LookupIntrospectHandler()
    {
      @POST
      public Response postMock(InputStream inputStream)
      {
        return Response.ok().build();
      }
    };

    LookupExtractorFactory lookupExtractorFactory1 = new LookupExtractorFactory()
    {
      final LookupExtractor mapLookup = new MapLookupExtractor(ImmutableMap.<String, String>of("key", "value"), true);

      @Override
      public boolean start()
      {
        return true;
      }

      @Override
      public boolean close()
      {
        return true;
      }

      @Override
      public boolean replaces(@Nullable LookupExtractorFactory other)
      {
        return true;
      }

      @Nullable
      @Override
      public LookupIntrospectHandler getIntrospectHandler()
      {
        return null;
      }

      @Override
      public LookupExtractor get()
      {
        return mapLookup;
      }
    };

    LookupIntrospectionResource lookupIntrospectionResource = new LookupIntrospectionResource(lookupReferencesManager);
    EasyMock.expect(lookupReferencesManager.get("lookupId1")).andReturn(
        new LookupExtractorFactoryContainer(
            "v0",
            lookupExtractorFactory1
        )
    ).anyTimes();
    EasyMock.replay(lookupReferencesManager);

  }
}
