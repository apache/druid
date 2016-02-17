/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.server.listener.resource;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.metamx.common.StringUtils;
import io.druid.jackson.DefaultObjectMapper;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


public class ListenerResourceTest
{
  static final String ANN_ID = "announce_id";
  HttpServletRequest req;
  final ObjectMapper mapper = new DefaultObjectMapper();
  private static final ByteSource EMPTY_JSON_LIST = new ByteSource()
  {
    @Override
    public InputStream openStream() throws IOException
    {
      return new ByteArrayInputStream(StringUtils.toUtf8("[]"));
    }
  };

  @Before
  public void setUp() throws Exception
  {
    mapper.registerSubtypes(SomeBeanClass.class);
    req = EasyMock.createNiceMock(HttpServletRequest.class);
    EasyMock.expect(req.getContentType()).andReturn(MediaType.APPLICATION_JSON).anyTimes();
    EasyMock.replay(req);
  }

  @After
  public void tearDown() throws Exception
  {

  }

  @Test
  public void testServiceAnnouncementPOSTExceptionInHandler() throws Exception
  {
    final ListenerHandler handler = EasyMock.createStrictMock(ListenerHandler.class);
    EasyMock.expect(handler.handlePOST(EasyMock.<InputStream>anyObject(), EasyMock.<ObjectMapper>anyObject())).andThrow(new RuntimeException("test"));
    final ListenerResource resource = new ListenerResource(
        mapper,
        mapper,
        handler
    )
    {
    };
    EasyMock.replay(handler);
    Assert.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
        resource.serviceAnnouncementPOST("id", EMPTY_JSON_LIST.openStream(), req).getStatus()
    );
    EasyMock.verify(req, handler);
  }

  @Test
  public void testServiceAnnouncementPOSTAllExceptionInHandler() throws Exception
  {
    final ListenerHandler handler = EasyMock.createStrictMock(ListenerHandler.class);
    EasyMock.expect(handler.handlePOSTAll(EasyMock.<InputStream>anyObject(), EasyMock.<ObjectMapper>anyObject())).andThrow(new RuntimeException("test"));
    final ListenerResource resource = new ListenerResource(
        mapper,
        mapper,
        handler
    )
    {
    };
    EasyMock.replay(handler);
    Assert.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
        resource.serviceAnnouncementPOSTAll(EMPTY_JSON_LIST.openStream(), req).getStatus()
    );
    EasyMock.verify(req, handler);
  }

  @Test
  public void testServiceAnnouncementPOST() throws Exception
  {
    final AtomicInteger c = new AtomicInteger(0);
    final ListenerResource resource = new ListenerResource(
        mapper,
        mapper,
        new ExceptionalAbstractListenerHandler()
        {
          @Override
          public Object post(List<SomeBeanClass> l)
          {
            c.incrementAndGet();
            return l;
          }
        }
    )
    {
    };
    Assert.assertEquals(
        202,
        resource.serviceAnnouncementPOSTAll(EMPTY_JSON_LIST.openStream(), req).getStatus()
    );
    Assert.assertEquals(1, c.get());
    EasyMock.verify(req);
  }

  @Test
  public void testServiceAnnouncementGET() throws Exception
  {
    final AtomicInteger c = new AtomicInteger(0);
    final AbstractListenerHandler handler = new ExceptionalAbstractListenerHandler()
    {
      @Override
      public Object get(String id)
      {
        c.incrementAndGet();
        return ANN_ID.equals(id) ? ANN_ID : null;
      }
    };
    final ListenerResource resource = new ListenerResource(
        mapper,
        mapper,
        handler
    )
    {
    };
    Assert.assertEquals(
        Response.Status.OK.getStatusCode(),
        resource.serviceAnnouncementGET(ANN_ID).getStatus()
    );
    Assert.assertEquals(1, c.get());
    EasyMock.verify(req);
  }


  @Test
  public void testServiceAnnouncementGETNull() throws Exception
  {
    final AbstractListenerHandler handler = new ExceptionalAbstractListenerHandler();
    final ListenerResource resource = new ListenerResource(
        mapper,
        mapper,
        handler
    )
    {
    };
    Assert.assertEquals(
        400,
        resource.serviceAnnouncementGET(null).getStatus()
    );
    Assert.assertEquals(
        400,
        resource.serviceAnnouncementGET("").getStatus()
    );
    EasyMock.verify(req);
  }

  @Test
  public void testServiceAnnouncementGETExceptionInHandler() throws Exception
  {
    final ListenerHandler handler = EasyMock.createStrictMock(ListenerHandler.class);
    EasyMock.expect(handler.handleGET(EasyMock.anyString())).andThrow(new RuntimeException("test"));
    final ListenerResource resource = new ListenerResource(
        mapper,
        mapper,
        handler
    )
    {
    };
    EasyMock.replay(handler);
    Assert.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
        resource.serviceAnnouncementGET("id").getStatus()
    );
    EasyMock.verify(handler);
  }

  @Test
  public void testServiceAnnouncementGETAllExceptionInHandler() throws Exception
  {
    final ListenerHandler handler = EasyMock.createStrictMock(ListenerHandler.class);
    EasyMock.expect(handler.handleGETAll()).andThrow(new RuntimeException("test"));
    final ListenerResource resource = new ListenerResource(
        mapper,
        mapper,
        handler
    )
    {
    };
    EasyMock.replay(handler);
    Assert.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
        resource.getAll().getStatus()
    );
    EasyMock.verify(handler);
  }

  @Test
  public void testServiceAnnouncementDELETENullID() throws Exception
  {
    final AbstractListenerHandler handler = new ExceptionalAbstractListenerHandler();
    final ListenerResource resource = new ListenerResource(
        mapper,
        mapper,
        handler
    )
    {
    };

    Assert.assertEquals(
        Response.Status.BAD_REQUEST.getStatusCode(),
        resource.serviceAnnouncementDELETE(null).getStatus()
    );
  }

  @Test
  public void testServiceAnnouncementDELETEExceptionInHandler() throws Exception
  {

    final ListenerHandler handler = EasyMock.createStrictMock(ListenerHandler.class);
    EasyMock.expect(handler.handleDELETE(EasyMock.anyString())).andThrow(new RuntimeException("test"));
    final ListenerResource resource = new ListenerResource(
        mapper,
        mapper,
        handler
    )
    {
    };
    EasyMock.replay(handler);
    Assert.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
        resource.serviceAnnouncementDELETE("id").getStatus()
    );
    EasyMock.verify(handler);
  }

  @Test
  public void testServiceAnnouncementDELETE() throws Exception
  {
    final AtomicInteger c = new AtomicInteger(0);
    final AbstractListenerHandler handler = new ExceptionalAbstractListenerHandler()
    {
      @Override
      public Object delete(String id)
      {
        c.incrementAndGet();
        return ANN_ID.equals(id) ? ANN_ID : null;
      }
    };
    final ListenerResource resource = new ListenerResource(
        mapper,
        mapper,
        handler
    )
    {
    };
    Assert.assertEquals(
        202,
        resource.serviceAnnouncementDELETE(ANN_ID).getStatus()
    );
    Assert.assertEquals(1, c.get());
    EasyMock.verify(req);
  }

  @Test
  // Take a list of strings wrap them in a JSON POJO and get them back as an array string in the POST function
  public void testAbstractPostHandler() throws Exception
  {
    final AbstractListenerHandler handler = new ExceptionalAbstractListenerHandler()
    {

      @Nullable
      @Override
      public String post(
          @NotNull List<SomeBeanClass> inputObject
      ) throws Exception
      {
        return Arrays.deepToString(
            Collections2.transform(
                inputObject, new Function<SomeBeanClass, String>()
                {
                  @Nullable
                  @Override
                  public String apply(
                      @Nullable SomeBeanClass input
                  )
                  {
                    if (input == null) {
                      throw new NullPointerException();
                    }
                    return input.getP();
                  }
                }
            ).toArray()
        );
      }
    };
    final ListenerResource resource = new ListenerResource(
        mapper,
        mapper,
        handler
    )
    {
    };
    final List<String> strings = ImmutableList.of("test1", "test2");
    final Response response = resource.serviceAnnouncementPOSTAll(
        new ByteArrayInputStream(
            StringUtils.toUtf8(
                mapper.writeValueAsString(
                    ImmutableList.copyOf(
                        Collections2.transform(
                            strings,
                            new Function<String, SomeBeanClass>()
                            {
                              @Nullable
                              @Override
                              public SomeBeanClass apply(String input)
                              {
                                return new SomeBeanClass(input);
                              }
                            }
                        )
                    )
                )
            )
        ),
        req
    );
    Assert.assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
    Assert.assertEquals(Arrays.deepToString(strings.toArray()), response.getEntity());
  }


  @Test
  public void testAbstractPostHandlerEmptyList() throws Exception
  {
    final AbstractListenerHandler handler = new ExceptionalAbstractListenerHandler()
    {
      @Override
      public String post(List<SomeBeanClass> inputObject) throws Exception
      {
        return Arrays.deepToString(
            Collections2.transform(
                inputObject, new Function<SomeBeanClass, String>()
                {
                  @Nullable
                  @Override
                  public String apply(
                      @Nullable SomeBeanClass input
                  )
                  {
                    throw new UnsupportedOperationException("Shouldn't have made it here");
                  }
                }
            ).toArray()
        );
      }
    };
    final ListenerResource resource = new ListenerResource(
        mapper,
        mapper,
        handler
    )
    {
    };
    final Response response = resource.serviceAnnouncementPOSTAll(
        EMPTY_JSON_LIST.openStream(),
        req
    );
    Assert.assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
    Assert.assertEquals("[]", response.getEntity());
  }


  @Test
  public void testAbstractPostHandlerException() throws Exception
  {
    final AbstractListenerHandler handler = new ExceptionalAbstractListenerHandler()
    {

      @Override
      public String post(List<SomeBeanClass> inputObject) throws Exception
      {
        throw new UnsupportedOperationException("nope!");
      }
    };
    final ListenerResource resource = new ListenerResource(
        mapper,
        mapper,
        handler
    )
    {
    };
    final List<String> strings = ImmutableList.of("test1", "test2");
    final Response response = resource.serviceAnnouncementPOSTAll(
        new ByteArrayInputStream(
            StringUtils.toUtf8(
                mapper.writeValueAsString(
                    Collections2.transform(
                        strings, new Function<String, SomeBeanClass>()
                        {
                          @Nullable
                          @Override
                          public SomeBeanClass apply(String input)
                          {
                            return new SomeBeanClass(input);
                          }
                        }
                    )
                )
            )
        ),
        req
    );
    Assert.assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
  }
}

@JsonTypeName("someBean")
class SomeBeanClass
{
  protected static final TypeReference<SomeBeanClass> TYPE_REFERENCE = new TypeReference<SomeBeanClass>()
  {
  };

  private final String p;

  @JsonCreator
  public SomeBeanClass(
      @JsonProperty("p") String p
  )
  {
    this.p = p;
  }

  @JsonProperty
  public String getP()
  {
    return this.p;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SomeBeanClass that = (SomeBeanClass) o;

    return p != null ? p.equals(that.p) : that.p == null;

  }

  @Override
  public int hashCode()
  {
    return p != null ? p.hashCode() : 0;
  }

  @Override
  public String toString()
  {
    return "SomeBeanClass{" +
           "p='" + p + '\'' +
           '}';
  }
}

class ExceptionalAbstractListenerHandler extends AbstractListenerHandler<SomeBeanClass>
{
  public ExceptionalAbstractListenerHandler()
  {
    super(SomeBeanClass.TYPE_REFERENCE);
  }

  @Nullable
  @Override
  protected Object delete(@NotNull String id)
  {
    throw new UnsupportedOperationException("should not have called DELETE");
  }

  @Nullable
  @Override
  protected Object get(@NotNull String id)
  {
    throw new UnsupportedOperationException("should not have called GET");
  }

  @Nullable
  @Override
  protected Collection<SomeBeanClass> getAll()
  {
    throw new UnsupportedOperationException("should not have called GET all");
  }

  @Nullable
  @Override
  public Object post(@NotNull List<SomeBeanClass> inputObject) throws Exception
  {
    throw new UnsupportedOperationException("should not have called post");
  }
}
