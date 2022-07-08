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

package org.apache.druid.storage.google;

import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.storage.Storage;
import com.google.common.base.Suppliers;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

public class GoogleStorageTest
{
  @Test
  public void testGet() throws IOException
  {
    String content = "abcdefghij";
    MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
    response.setContent(content);
    GoogleStorage googleStorage = makeGoogleStorage(response);
    InputStream is = googleStorage.get("bucket", "path");
    String actual = GoogleTestUtils.readAsString(is);
    Assert.assertEquals(content, actual);
  }

  @Test
  public void testGetWithOffset() throws IOException
  {
    String content = "abcdefghij";
    MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
    response.setContent(content);
    GoogleStorage googleStorage = makeGoogleStorage(response);
    InputStream is = googleStorage.get("bucket", "path", 2);
    String actual = GoogleTestUtils.readAsString(is);
    Assert.assertEquals(content.substring(2), actual);
  }

  @Test
  public void testInsert() throws IOException
  {
    String content = "abcdefghij";
    MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
    response.addHeader("Location", "http://random-path");
    response.setContent("{}");
    MockHttpTransport transport = new MockHttpTransport.Builder().setLowLevelHttpResponse(response).build();
    GoogleStorage googleStorage = makeGoogleStorage(transport);
    googleStorage.insert("bucket", "path", new ByteArrayContent("text/html", StringUtils.toUtf8(content)));
    MockLowLevelHttpRequest request = transport.getLowLevelHttpRequest();
    String actual = request.getContentAsString();
    Assert.assertEquals(content, actual);
  }

  private GoogleStorage makeGoogleStorage(MockLowLevelHttpResponse response)
  {
    MockHttpTransport transport = new MockHttpTransport.Builder().setLowLevelHttpResponse(response).build();
    return makeGoogleStorage(transport);
  }

  private GoogleStorage makeGoogleStorage(MockHttpTransport transport)
  {
    HttpRequestInitializer initializer = new MockGoogleCredential.Builder().build();
    Storage storage = new Storage(transport, JacksonFactory.getDefaultInstance(), initializer);
    return new GoogleStorage(Suppliers.ofInstance(storage));
  }
}
