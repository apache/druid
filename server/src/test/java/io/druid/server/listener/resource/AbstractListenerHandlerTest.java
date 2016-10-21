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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.StringUtils;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class AbstractListenerHandlerTest
{
  final ObjectMapper mapper = new DefaultObjectMapper();
  final AtomicBoolean failPost = new AtomicBoolean(false);
  final String error_msg = "err message";

  final Object good_object = new Object();
  final AtomicBoolean shouldFail = new AtomicBoolean(false);
  final AtomicBoolean returnEmpty = new AtomicBoolean(false);
  final String error_message = "some error message";
  final String good_id = "good id";
  final String error_id = "error id";
  final Map<String, SomeBeanClass> all = ImmutableMap.of();


  final Object obj = new Object();
  final String valid_id = "some_id";

  final AbstractListenerHandler<SomeBeanClass> abstractListenerHandler =
      new AbstractListenerHandler<SomeBeanClass>(SomeBeanClass.TYPE_REFERENCE)
      {
        @Nullable
        @Override
        public Object post(@NotNull Map<String, SomeBeanClass> inputObject) throws Exception
        {
          if (failPost.get()) {
            throw new Exception(error_msg);
          }
          return inputObject.isEmpty() ? null : inputObject;
        }

        @Nullable
        @Override
        protected Object get(@NotNull String id)
        {
          if (error_id.equals(id)) {
            throw new RuntimeException(error_message);
          }
          return good_id.equals(id) ? good_object : null;
        }

        @Nullable
        @Override
        protected Map<String, SomeBeanClass> getAll()
        {
          if (shouldFail.get()) {
            throw new RuntimeException(error_message);
          }
          return returnEmpty.get() ? null : all;
        }

        @Nullable
        @Override
        protected Object delete(@NotNull String id)
        {
          if (error_id.equals(id)) {
            throw new RuntimeException(error_msg);
          }
          return valid_id.equals(id) ? obj : null;
        }
      };

  @Before
  public void setUp()
  {
    mapper.registerSubtypes(SomeBeanClass.class);
  }

  @Test
  public void testSimple() throws Exception
  {
    final SomeBeanClass val = new SomeBeanClass("a");
    final ByteArrayInputStream bais = new ByteArrayInputStream(StringUtils.toUtf8(mapper.writeValueAsString(val)));
    final Response response = abstractListenerHandler.handlePOST(bais, mapper, good_id);
    Assert.assertEquals(202, response.getStatus());
    Assert.assertEquals(ImmutableMap.of(good_id, val), response.getEntity());
  }


  @Test
  public void testSimpleAll() throws Exception
  {
    final Map<String, SomeBeanClass> val = ImmutableMap.of("a", new SomeBeanClass("a"));
    final ByteArrayInputStream bais = new ByteArrayInputStream(
        StringUtils.toUtf8(
            mapper.writeValueAsString(
                val
            )
        )
    );
    final Response response = abstractListenerHandler.handlePOSTAll(bais, mapper);
    Assert.assertEquals(202, response.getStatus());
    Assert.assertEquals(val, response.getEntity());
  }

  @Test
  public void testMissingAll() throws Exception
  {
    final Map<String, SomeBeanClass> val = ImmutableMap.of();
    final ByteArrayInputStream bais = new ByteArrayInputStream(
        StringUtils.toUtf8(
            mapper.writeValueAsString(
                val
            )
        )
    );
    final Response response = abstractListenerHandler.handlePOSTAll(bais, mapper);
    Assert.assertEquals(404, response.getStatus());
  }

  @Test
  public void testErrorAll() throws Exception
  {
    final Map<String, SomeBeanClass> val = ImmutableMap.of();
    final ByteArrayInputStream bais = new ByteArrayInputStream(
        StringUtils.toUtf8(
            mapper.writeValueAsString(
                val
            )
        )
    );
    failPost.set(true);
    final Response response = abstractListenerHandler.handlePOSTAll(bais, mapper);
    Assert.assertEquals(500, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", error_msg), response.getEntity());
  }

  @Test
  public void testError() throws Exception
  {
    final ByteArrayInputStream bais = new ByteArrayInputStream(StringUtils.toUtf8(mapper.writeValueAsString(new SomeBeanClass(
        "a"))));
    failPost.set(true);
    final Response response = abstractListenerHandler.handlePOST(bais, mapper, good_id);
    Assert.assertEquals(500, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", error_msg), response.getEntity());
  }

  @Test
  public void testBadInput() throws Exception
  {
    final ByteArrayInputStream bais = new ByteArrayInputStream(new byte[]{0, 0, 0});
    final Response response = abstractListenerHandler.handlePOST(bais, mapper, good_id);
    Assert.assertEquals(400, response.getStatus());
  }

  @Test
  public void testBadInnerInput() throws Exception
  {
    final ByteArrayInputStream bais = new ByteArrayInputStream(new byte[]{});
    final ObjectMapper mapper = EasyMock.createStrictMock(ObjectMapper.class);
    EasyMock.expect(mapper.readValue(EasyMock.<InputStream>anyObject(), EasyMock.<TypeReference<Object>>anyObject()))
            .andThrow(new IOException());
    EasyMock.replay(mapper);
    final Response response = abstractListenerHandler.handlePOSTAll(bais, mapper);
    Assert.assertEquals(400, response.getStatus());
    EasyMock.verify(mapper);
  }


  @Test
  public void testHandleSimpleDELETE() throws Exception
  {
    final Response response = abstractListenerHandler.handleDELETE(valid_id);
    Assert.assertEquals(202, response.getStatus());
    Assert.assertEquals(obj, response.getEntity());
  }

  @Test
  public void testMissingDELETE() throws Exception
  {
    final Response response = abstractListenerHandler.handleDELETE("not going to find it");
    Assert.assertEquals(404, response.getStatus());
  }

  @Test
  public void testErrorDELETE() throws Exception
  {
    final Response response = abstractListenerHandler.handleDELETE(error_id);
    Assert.assertEquals(500, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", error_msg), response.getEntity());
  }

  @Test
  public void testHandle() throws Exception
  {
    final Response response = abstractListenerHandler.handleGET(good_id);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(good_object, response.getEntity());
  }

  @Test
  public void testMissingHandle() throws Exception
  {
    final Response response = abstractListenerHandler.handleGET("neva gonna get it");
    Assert.assertEquals(404, response.getStatus());
  }

  @Test
  public void testExceptionalHandle() throws Exception
  {
    final Response response = abstractListenerHandler.handleGET(error_id);
    Assert.assertEquals(500, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", error_message), response.getEntity());
  }

  @Test
  public void testHandleAll() throws Exception
  {
    final Response response = abstractListenerHandler.handleGETAll();
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(all, response.getEntity());
  }

  @Test
  public void testExceptionalHandleAll() throws Exception
  {
    shouldFail.set(true);
    final Response response = abstractListenerHandler.handleGETAll();
    Assert.assertEquals(500, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", error_message), response.getEntity());
  }

  @Test
  public void testMissingHandleAll() throws Exception
  {
    returnEmpty.set(true);
    final Response response = abstractListenerHandler.handleGETAll();
    Assert.assertEquals(404, response.getStatus());
  }
}
