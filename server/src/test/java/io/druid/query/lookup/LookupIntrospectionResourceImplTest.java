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
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.core.ClassNamesResourceConfig;
import com.sun.jersey.spi.container.servlet.WebComponent;
import com.sun.jersey.spi.inject.SingletonTypeInjectableProvider;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;
import com.sun.jersey.test.framework.spi.container.TestContainerFactory;
import com.sun.jersey.test.framework.spi.container.grizzly2.GrizzlyTestContainerFactory;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;

public class LookupIntrospectionResourceImplTest extends JerseyTest
{

   static LookupReferencesManager lookupReferencesManager = EasyMock.createMock(LookupReferencesManager.class);


  @Override
  @Before
  public void setUp() throws Exception
  {
    super.setUp();
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

  @Provider
  public static class MockTodoServiceProvider extends
      SingletonTypeInjectableProvider<Context, LookupReferencesManager>
  {
    public MockTodoServiceProvider()
    {
      super(LookupReferencesManager.class, lookupReferencesManager);
    }
  }


  public LookupIntrospectionResourceImplTest()
  {
    super(new WebAppDescriptor.Builder().initParam(
                                            WebComponent.RESOURCE_CONFIG_CLASS,
                                            ClassNamesResourceConfig.class.getName()
                                        )
                                        .initParam(
                                            ClassNamesResourceConfig.PROPERTY_CLASSNAMES,
                                            LookupIntrospectionResource.class.getName()
                                            + ';'
                                            + MockTodoServiceProvider.class.getName()
                                            + ';'
                                            + LookupIntrospectHandler.class.getName()
                                        )
                                        .build());
  }

  @Override
  protected TestContainerFactory getTestContainerFactory()
  {
    return new GrizzlyTestContainerFactory();
  }


  @Test
  public void testGetKey()
  {

    WebResource r = resource().path("/druid/v1/lookups/introspect/lookupId1/keys");
    String s = r.get(String.class);
    Assert.assertEquals("[key, key2]", s);
  }

  @Test
  public void testGetValue()
  {
    WebResource r = resource().path("/druid/v1/lookups/introspect/lookupId1/values");
    String s = r.get(String.class);
    Assert.assertEquals("[value, value2]", s);
  }

  @Test
  public void testGetMap()
  {
    WebResource r = resource().path("/druid/v1/lookups/introspect/lookupId1/");
    String s = r.get(String.class);
    Assert.assertEquals("{\"key\":\"value\",\"key2\":\"value2\"}", s);
  }
}
