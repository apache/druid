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
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.server.WebserverTestUtils;
import org.easymock.EasyMock;
import org.glassfish.grizzly.http.server.HttpServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;

public class LookupIntrospectionResourceTest
{
  private static final LookupExtractorFactory MOCK_LOOKUP_EXTRACTOR_FACTORY = EasyMock.createMock(LookupExtractorFactory.class);

  private static final LookupExtractorFactoryContainerProvider MOCK_LOOKUP_EXTRACTOR_FACTORY_CONTAINER_PROVIDER = EasyMock.createMock(LookupExtractorFactoryContainerProvider.class);

  private static final LookupIntrospectHandler MOCK_LOOKUP_INTROSPECT_HANDLER = EasyMock.createMock(LookupIntrospectHandler.class);

  private LookupIntrospectionResource lookupIntrospectionResource =
      new LookupIntrospectionResource(MOCK_LOOKUP_EXTRACTOR_FACTORY_CONTAINER_PROVIDER);

  private URI baseUri;
  private HttpServer server;

  @Before
  public void setup() throws Exception
  {
    LookupExtractorFactory actualLookupExtractorFactory = new MapLookupExtractorFactory(
        ImmutableMap.of("key", "value", "key2", "value2"),
        false
    );

    EasyMock.reset(MOCK_LOOKUP_EXTRACTOR_FACTORY_CONTAINER_PROVIDER);
    EasyMock.reset(MOCK_LOOKUP_EXTRACTOR_FACTORY);
    EasyMock.reset(MOCK_LOOKUP_INTROSPECT_HANDLER);
    EasyMock.expect(MOCK_LOOKUP_EXTRACTOR_FACTORY_CONTAINER_PROVIDER.get("lookupId")).andReturn(
        new LookupExtractorFactoryContainer(
            "v0",
            MOCK_LOOKUP_EXTRACTOR_FACTORY
        )
    ).anyTimes();
    EasyMock.expect(MOCK_LOOKUP_EXTRACTOR_FACTORY_CONTAINER_PROVIDER.get("lookupId1")).andReturn(
        new LookupExtractorFactoryContainer(
            "v0",
            actualLookupExtractorFactory
        )
    ).anyTimes();

    EasyMock.expect(MOCK_LOOKUP_EXTRACTOR_FACTORY_CONTAINER_PROVIDER.get(EasyMock.anyString())).andReturn(null).anyTimes();
    EasyMock.replay(MOCK_LOOKUP_EXTRACTOR_FACTORY_CONTAINER_PROVIDER);

    baseUri = WebserverTestUtils.createBaseUri();
    server = WebserverTestUtils.createServer(
        "lookup-test",
        baseUri,
        LookupIntrospectionResource.class.getName(),
        binder -> {
          binder.bind(LookupExtractorFactoryContainerProvider.class)
                .toInstance(MOCK_LOOKUP_EXTRACTOR_FACTORY_CONTAINER_PROVIDER);
        }
    );
    server.start();
  }

  @After
  public void teardown()
  {
    if (server != null) {
      server.stop();
    }
  }

  @Test
  public void testNotImplementedIntrospectLookup()
  {
    EasyMock.expect(MOCK_LOOKUP_EXTRACTOR_FACTORY.getIntrospectHandler()).andReturn(null);
    EasyMock.expect(MOCK_LOOKUP_EXTRACTOR_FACTORY.get())
            .andReturn(new MapLookupExtractor(ImmutableMap.of(), false))
            .anyTimes();
    EasyMock.replay(MOCK_LOOKUP_EXTRACTOR_FACTORY);
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
    EasyMock.expect(MOCK_LOOKUP_EXTRACTOR_FACTORY.getIntrospectHandler()).andReturn(MOCK_LOOKUP_INTROSPECT_HANDLER);
    EasyMock.expect(MOCK_LOOKUP_EXTRACTOR_FACTORY.get())
            .andReturn(new MapLookupExtractor(ImmutableMap.of(), false))
            .anyTimes();
    EasyMock.replay(MOCK_LOOKUP_EXTRACTOR_FACTORY);
    Assert.assertEquals(MOCK_LOOKUP_INTROSPECT_HANDLER, lookupIntrospectionResource.introspectLookup("lookupId"));
  }

  @Test
  public void testGetKey()
  {
    Client client = Client.create(new DefaultClientConfig());
    WebResource service = client.resource(baseUri);

    ClientResponse resp = service.path("/druid/v1/lookups/introspect/lookupId1/keys")
                                 .accept(MediaType.APPLICATION_JSON)
                                 .get(ClientResponse.class);
    String s = resp.getEntity(String.class);
    Assert.assertEquals("[key, key2]", s);
    Assert.assertEquals(200, resp.getStatus());
  }

  @Test
  public void testGetValue()
  {
    Client client = Client.create(new DefaultClientConfig());
    WebResource service = client.resource(baseUri);

    ClientResponse resp = service.path("/druid/v1/lookups/introspect/lookupId1/values")
                                 .accept(MediaType.APPLICATION_JSON)
                                 .get(ClientResponse.class);
    String s = resp.getEntity(String.class);
    Assert.assertEquals("[value, value2]", s);
    Assert.assertEquals(200, resp.getStatus());
  }

  @Test
  public void testGetMap()
  {
    Client client = Client.create(new DefaultClientConfig());
    WebResource service = client.resource(baseUri);

    ClientResponse resp = service.path("/druid/v1/lookups/introspect/lookupId1/")
                                 .accept(MediaType.APPLICATION_JSON)
                                 .get(ClientResponse.class);
    String s = resp.getEntity(String.class);
    Assert.assertEquals("{\"key\":\"value\",\"key2\":\"value2\"}", s);
    Assert.assertEquals(200, resp.getStatus());
  }
}
