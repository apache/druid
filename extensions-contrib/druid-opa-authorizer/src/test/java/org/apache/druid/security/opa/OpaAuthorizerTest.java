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

package org.apache.druid.security.opa;

import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.naming.directory.BasicAttributes;
import javax.naming.directory.SearchResult;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Flow;

public class OpaAuthorizerTest
{
  private HttpClient httpClient;
  private OpaAuthorizer opaAuthorizer;
  private static final String OPA_URI = "http://localhost:8181/v1/data/druid/allow";

  // Helper to extract body from HttpRequest for testing
  private static String getBody(HttpRequest request)
  {
    if (request.bodyPublisher().isEmpty()) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    request.bodyPublisher().get().subscribe(new Flow.Subscriber<>()
    {
      @Override
      public void onSubscribe(Flow.Subscription subscription)
      {
        subscription.request(Long.MAX_VALUE);
      }

      @Override
      public void onNext(ByteBuffer item)
      {
        byte[] bytes = new byte[item.remaining()];
        item.get(bytes);
        sb.append(new String(bytes, StandardCharsets.UTF_8));
      }

      @Override
      public void onError(Throwable throwable)
      {
      }

      @Override
      public void onComplete()
      {
      }
    });
    return sb.toString();
  }

  @Before
  public void setUp()
  {
    httpClient = Mockito.mock(HttpClient.class);
    opaAuthorizer = new OpaAuthorizer("opa", OPA_URI, httpClient);
  }

  @Test
  public void testAuthorizeAllowed() throws Exception
  {
    @SuppressWarnings("unchecked")
    HttpResponse<String> response = Mockito.mock(HttpResponse.class);
    Mockito.when(response.statusCode()).thenReturn(200);
    Mockito.when(response.body()).thenReturn("{\"result\": true}");
    Mockito.when(httpClient.send(ArgumentMatchers.any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any()))
           .thenReturn(response);

    AuthenticationResult authResult = new AuthenticationResult("user", "authorizer", "authenticator", null);
    Resource resource = new Resource("dataSource", ResourceType.DATASOURCE);
    Access access = opaAuthorizer.authorize(authResult, resource, Action.READ);

    Assert.assertTrue(access.isAllowed());
  }

  @Test
  public void testAuthorizeDenied() throws Exception
  {
    @SuppressWarnings("unchecked")
    HttpResponse<String> response = Mockito.mock(HttpResponse.class);
    Mockito.when(response.statusCode()).thenReturn(200);
    Mockito.when(response.body()).thenReturn("{\"result\": false}");
    Mockito.when(httpClient.send(ArgumentMatchers.any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any()))
           .thenReturn(response);

    AuthenticationResult authResult = new AuthenticationResult("user", "authorizer", "authenticator", null);
    Resource resource = new Resource("dataSource", ResourceType.DATASOURCE);
    Access access = opaAuthorizer.authorize(authResult, resource, Action.READ);

    Assert.assertFalse(access.isAllowed());
    Assert.assertEquals("Unauthorized, Access denied.", access.getMessage());
  }

  @Test
  public void testAuthorizeError() throws Exception
  {
    Mockito.when(httpClient.send(ArgumentMatchers.any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any()))
           .thenThrow(new RuntimeException("Network error"));

    AuthenticationResult authResult = new AuthenticationResult("user", "authorizer", "authenticator", null);
    Resource resource = new Resource("dataSource", ResourceType.DATASOURCE);
    Access access = opaAuthorizer.authorize(authResult, resource, Action.READ);

    Assert.assertFalse(access.isAllowed());
    Assert.assertTrue(access.getMessage().contains("Unauthorized, An error occurred: java.lang.RuntimeException: Network error"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidUri()
  {
    OpaAuthorizer ignored = new OpaAuthorizer("opa", "invalid uri", httpClient);
    Assert.assertNotNull(ignored);
  }

  @Test
  public void testAuthorizeWithNonSerializableContext() throws Exception
  {
    @SuppressWarnings("unchecked")
    HttpResponse<String> response = Mockito.mock(HttpResponse.class);
    Mockito.when(response.statusCode()).thenReturn(200);
    Mockito.when(response.body()).thenReturn("{\"result\": true}");
    
    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    Mockito.when(httpClient.send(requestCaptor.capture(), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any()))
           .thenReturn(response);

    // Mimic LDAP SearchResult which has non-serializable elements
    BasicAttributes attributes = new BasicAttributes();
    attributes.put("uid", "user");
    attributes.put("memberOf", "cn=group1,ou=Groups,dc=example,dc=org");
    attributes.add("memberOf", "cn=group2,ou=Groups,dc=example,dc=org");
    byte[] photoBytes = new byte[]{1, 2, 3};
    attributes.put("jpegPhoto", photoBytes);
    SearchResult searchResult = new SearchResult("uid=user", "java.lang.Object", null, attributes, false);
    searchResult.setNameInNamespace("dc=example,dc=org");

    Map<String, Object> context = new HashMap<>();
    context.put("searchResult", searchResult);

    AuthenticationResult authResult = new AuthenticationResult("user", "authorizer", "authenticator", context);
    Resource resource = new Resource("dataSource", ResourceType.DATASOURCE);
    Access access = opaAuthorizer.authorize(authResult, resource, Action.READ);

    Assert.assertTrue(access.isAllowed());
    
    HttpRequest capturedRequest = requestCaptor.getValue();
    String requestBody = getBody(capturedRequest);
    
    Assert.assertTrue(requestBody.contains("\"name\":\"uid=user\""));
    Assert.assertTrue(requestBody.contains("\"nameInNamespace\":\"dc=example,dc=org\""));
    Assert.assertTrue(requestBody.contains("\"uid\":[\"user\"]"));
    Assert.assertTrue(requestBody.contains("\"memberof\":[\"cn=group1,ou=Groups,dc=example,dc=org\",\"cn=group2,ou=Groups,dc=example,dc=org\"]"));
    Assert.assertTrue(requestBody.contains("\"jpegphoto\":[\"AQID\"]")); // Base64 for [1, 2, 3]
  }
}
