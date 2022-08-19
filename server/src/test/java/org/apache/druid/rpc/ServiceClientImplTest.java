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

package org.apache.druid.rpc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.ObjectOrErrorResponseHandler;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.OngoingStubbing;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ServiceClientImplTest
{
  private static final String SERVICE_NAME = "test-service";
  private static final ServiceLocation SERVER1 = new ServiceLocation("example.com", -1, 8888, "/q");
  private static final ServiceLocation SERVER2 = new ServiceLocation("example.com", -1, 9999, "/q");

  private ScheduledExecutorService exec;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock
  private HttpClient httpClient;

  @Mock
  private ServiceLocator serviceLocator;

  private ServiceClient serviceClient;

  @Before
  public void setUp()
  {
    exec = new NoDelayScheduledExecutorService(Execs.directExecutor());
  }

  @After
  public void tearDown() throws Exception
  {
    exec.shutdownNow();

    if (!exec.awaitTermination(30, TimeUnit.SECONDS)) {
      throw new ISE("Unable to shutdown executor in time");
    }
  }

  @Test
  public void test_request_ok() throws Exception
  {
    final RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.GET, "/foo");
    final ImmutableMap<String, String> expectedResponseObject = ImmutableMap.of("foo", "bar");

    // OK response from SERVER1.
    stubLocatorCall(locations(SERVER1, SERVER2));
    expectHttpCall(requestBuilder, SERVER1).thenReturn(valueResponse(expectedResponseObject));

    serviceClient = makeServiceClient(StandardRetryPolicy.noRetries());
    final Map<String, String> response = doRequest(serviceClient, requestBuilder);

    Assert.assertEquals(expectedResponseObject, response);
  }

  @Test
  public void test_request_serverError()
  {
    final RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.GET, "/foo");

    // Error response from SERVER1.
    stubLocatorCall(locations(SERVER1, SERVER2));
    expectHttpCall(requestBuilder, SERVER1)
        .thenReturn(errorResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, null, "oh no"));

    serviceClient = makeServiceClient(StandardRetryPolicy.builder().maxAttempts(2).build());

    final ExecutionException e = Assert.assertThrows(
        ExecutionException.class,
        () -> doRequest(serviceClient, requestBuilder)
    );

    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(HttpResponseException.class));

    final HttpResponseException httpResponseException = (HttpResponseException) e.getCause();
    Assert.assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, httpResponseException.getResponse().getStatus());
    Assert.assertEquals("oh no", httpResponseException.getResponse().getContent());
  }

  @Test
  public void test_request_serverErrorRetry() throws Exception
  {
    final RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.GET, "/foo");
    final ImmutableMap<String, String> expectedResponseObject = ImmutableMap.of("foo", "bar");

    // Error response from SERVER1, then OK response.
    stubLocatorCall(locations(SERVER1, SERVER2));
    expectHttpCall(requestBuilder, SERVER1)
        .thenReturn(errorResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, null, "oh no"))
        .thenReturn(valueResponse(expectedResponseObject));

    serviceClient = makeServiceClient(StandardRetryPolicy.unlimited());
    final Map<String, String> response = doRequest(serviceClient, requestBuilder);
    Assert.assertEquals(expectedResponseObject, response);
  }

  @Test
  public void test_request_ioError()
  {
    final RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.GET, "/foo");

    // IOException when contacting SERVER1.
    stubLocatorCall(locations(SERVER1, SERVER2));
    expectHttpCall(requestBuilder, SERVER1)
        .thenReturn(Futures.immediateFailedFuture(new IOException("oh no")));

    serviceClient = makeServiceClient(StandardRetryPolicy.builder().maxAttempts(2).build());

    final ExecutionException e = Assert.assertThrows(
        ExecutionException.class,
        () -> doRequest(serviceClient, requestBuilder)
    );

    MatcherAssert.assertThat(
        e.getCause(),
        ThrowableMessageMatcher.hasMessage(
            CoreMatchers.containsString(
                "Service [test-service] request [GET https://example.com:8888/q/foo] encountered exception on attempt #2"
            )
        )
    );
    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(RpcException.class));
    MatcherAssert.assertThat(e.getCause().getCause(), CoreMatchers.instanceOf(IOException.class));
    MatcherAssert.assertThat(
        e.getCause().getCause(),
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("oh no"))
    );
  }

  @Test
  public void test_request_ioErrorRetry() throws Exception
  {
    final RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.GET, "/foo");
    final ImmutableMap<String, String> expectedResponseObject = ImmutableMap.of("foo", "bar");

    // IOException when contacting SERVER1, then OK response.
    stubLocatorCall(locations(SERVER1, SERVER2));
    expectHttpCall(requestBuilder, SERVER1)
        .thenReturn(Futures.immediateFailedFuture(new IOException("oh no")))
        .thenReturn(valueResponse(expectedResponseObject));

    serviceClient = makeServiceClient(StandardRetryPolicy.unlimited());
    final Map<String, String> response = doRequest(serviceClient, requestBuilder);

    Assert.assertEquals(expectedResponseObject, response);
  }

  @Test
  public void test_request_nullResponseFromClient()
  {
    final RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.GET, "/foo");

    // Null response when contacting SERVER1. (HttpClient does this if an exception is encountered during processing.)
    stubLocatorCall(locations(SERVER1, SERVER2));
    expectHttpCall(requestBuilder, SERVER1).thenReturn(Futures.immediateFuture(null));

    serviceClient = makeServiceClient(StandardRetryPolicy.builder().maxAttempts(2).build());

    final ExecutionException e = Assert.assertThrows(
        ExecutionException.class,
        () -> doRequest(serviceClient, requestBuilder)
    );

    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(RpcException.class));
    MatcherAssert.assertThat(
        e.getCause(),
        ThrowableMessageMatcher.hasMessage(
            CoreMatchers.containsString(
                "Service [test-service] request [GET https://example.com:8888/q/foo] encountered exception on attempt #2"
            )
        )
    );
  }

  @Test
  public void test_request_nullResponseFromClientRetry() throws Exception
  {
    final RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.GET, "/foo");
    final ImmutableMap<String, String> expectedResponseObject = ImmutableMap.of("foo", "bar");

    // Null response when contacting SERVER1. (HttpClient does this if an exception is encountered during processing.)
    // Then, OK response.
    stubLocatorCall(locations(SERVER1, SERVER2));
    expectHttpCall(requestBuilder, SERVER1)
        .thenReturn(Futures.immediateFuture(null))
        .thenReturn(valueResponse(expectedResponseObject));

    serviceClient = makeServiceClient(StandardRetryPolicy.unlimited());
    final Map<String, String> response = doRequest(serviceClient, requestBuilder);

    Assert.assertEquals(expectedResponseObject, response);
  }

  @Test
  public void test_request_followRedirect() throws Exception
  {
    final RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.GET, "/foo");
    final ImmutableMap<String, String> expectedResponseObject = ImmutableMap.of("foo", "bar");

    // Redirect from SERVER1 -> SERVER2.
    stubLocatorCall(locations(SERVER1, SERVER2));
    expectHttpCall(requestBuilder, SERVER1)
        .thenReturn(redirectResponse(requestBuilder.build(SERVER2).getUrl().toString()));
    expectHttpCall(requestBuilder, SERVER2).thenReturn(valueResponse(expectedResponseObject));

    serviceClient = makeServiceClient(StandardRetryPolicy.noRetries());
    final Map<String, String> response = doRequest(serviceClient, requestBuilder);

    Assert.assertEquals(expectedResponseObject, response);
  }

  @Test
  public void test_request_tooManyRedirects()
  {
    final RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.GET, "/foo");

    // Endless self-redirects.
    stubLocatorCall(locations(SERVER1));
    expectHttpCall(requestBuilder, SERVER1)
        .thenReturn(redirectResponse(requestBuilder.build(SERVER1).getUrl().toString()));

    serviceClient = makeServiceClient(StandardRetryPolicy.unlimited());

    final ExecutionException e = Assert.assertThrows(
        ExecutionException.class,
        () -> doRequest(serviceClient, requestBuilder)
    );

    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(RpcException.class));
    MatcherAssert.assertThat(
        e.getCause(),
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("redirected too many times"))
    );
  }

  @Test
  public void test_request_redirectInvalid()
  {
    final RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.GET, "/foo");

    // Endless self-redirects.
    stubLocatorCall(locations(SERVER1));
    expectHttpCall(requestBuilder, SERVER1)
        .thenReturn(redirectResponse("invalid-url"));

    serviceClient = makeServiceClient(StandardRetryPolicy.unlimited());

    final ExecutionException e = Assert.assertThrows(
        ExecutionException.class,
        () -> doRequest(serviceClient, requestBuilder)
    );

    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(RpcException.class));
    MatcherAssert.assertThat(
        e.getCause(),
        ThrowableMessageMatcher.hasMessage(
            CoreMatchers.containsString("redirected [0] times to invalid URL [invalid-url]"))
    );
  }

  @Test
  public void test_request_redirectNil()
  {
    final RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.GET, "/foo");

    // Endless self-redirects.
    stubLocatorCall(locations(SERVER1));
    expectHttpCall(requestBuilder, SERVER1)
        .thenReturn(errorResponse(HttpResponseStatus.TEMPORARY_REDIRECT, null, null));

    serviceClient = makeServiceClient(StandardRetryPolicy.unlimited());

    final ExecutionException e = Assert.assertThrows(
        ExecutionException.class,
        () -> doRequest(serviceClient, requestBuilder)
    );

    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(RpcException.class));
    MatcherAssert.assertThat(
        e.getCause(),
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("redirected [0] times to invalid URL [null]"))
    );
  }

  @Test
  public void test_request_dontFollowRedirectToUnknownServer()
  {
    final RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.GET, "/foo");

    // Redirect from SERVER1 -> SERVER2, but SERVER2 is unknown.
    stubLocatorCall(locations(SERVER1));
    expectHttpCall(requestBuilder, SERVER1)
        .thenReturn(redirectResponse(requestBuilder.build(SERVER2).getUrl().toString()));

    serviceClient = makeServiceClient(StandardRetryPolicy.noRetries());

    final ExecutionException e = Assert.assertThrows(
        ExecutionException.class,
        () -> doRequest(serviceClient, requestBuilder)
    );

    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(RpcException.class));
    MatcherAssert.assertThat(
        e.getCause(),
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("redirected too many times"))
    );
  }

  @Test
  public void test_request_serviceUnavailable()
  {
    final RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.GET, "/foo");

    // Service unavailable.
    stubLocatorCall(locations());

    serviceClient = makeServiceClient(StandardRetryPolicy.noRetries());

    final ExecutionException e = Assert.assertThrows(
        ExecutionException.class,
        () -> doRequest(serviceClient, requestBuilder)
    );

    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(ServiceNotAvailableException.class));
    MatcherAssert.assertThat(
        e.getCause(),
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("Service [test-service] is not available"))
    );
  }

  @Test
  public void test_request_serviceUnavailableRetry() throws Exception
  {
    final RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.GET, "/foo");
    final ImmutableMap<String, String> expectedResponseObject = ImmutableMap.of("foo", "bar");

    // Service unavailable at first, then available.
    Mockito.when(serviceLocator.locate())
           .thenReturn(Futures.immediateFuture(locations()))
           .thenReturn(Futures.immediateFuture(locations(SERVER1)));
    expectHttpCall(requestBuilder, SERVER1).thenReturn(valueResponse(expectedResponseObject));

    serviceClient = makeServiceClient(StandardRetryPolicy.builder().maxAttempts(2).build());
    final Map<String, String> response = doRequest(serviceClient, requestBuilder);

    Assert.assertEquals(expectedResponseObject, response);
  }

  @Test
  public void test_request_serviceClosed()
  {
    final RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.GET, "/foo");

    // Closed service.
    stubLocatorCall(ServiceLocations.closed());

    // Closed services are not retryable.
    // Use an unlimited retry policy to ensure that the future actually resolves.
    serviceClient = makeServiceClient(StandardRetryPolicy.unlimited());

    final ExecutionException e = Assert.assertThrows(
        ExecutionException.class,
        () -> doRequest(serviceClient, requestBuilder)
    );

    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(ServiceClosedException.class));
    MatcherAssert.assertThat(
        e.getCause(),
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("Service [test-service] is closed"))
    );
  }

  @Test
  public void test_request_serviceLocatorException()
  {
    final RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.GET, "/foo");

    // Service locator returns a bad future.
    stubLocatorCall(Futures.immediateFailedFuture(new ISE("oh no")));

    // Service locator exceptions are not retryable.
    // Use an unlimited retry policy to ensure that the future actually resolves.
    serviceClient = makeServiceClient(StandardRetryPolicy.unlimited());

    final ExecutionException e = Assert.assertThrows(
        ExecutionException.class,
        () -> doRequest(serviceClient, requestBuilder)
    );

    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(RpcException.class));
    MatcherAssert.assertThat(e.getCause().getCause(), CoreMatchers.instanceOf(IllegalStateException.class));
    MatcherAssert.assertThat(
        e.getCause(),
        ThrowableMessageMatcher.hasMessage(
            CoreMatchers.containsString("Service [test-service] locator encountered exception")
        )
    );
    MatcherAssert.assertThat(
        e.getCause().getCause(),
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("oh no"))
    );
  }

  @Test
  public void test_request_cancelBeforeServiceLocated()
  {
    final RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.GET, "/foo");

    // Service that will never be located.
    stubLocatorCall(SettableFuture.create());

    serviceClient = makeServiceClient(StandardRetryPolicy.unlimited());

    final ListenableFuture<Map<String, String>> response = doAsyncRequest(serviceClient, requestBuilder);

    Assert.assertTrue(response.cancel(true));
    Assert.assertTrue(response.isCancelled());
  }

  @Test
  public void test_request_cancelDuringRetry()
  {
    final RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.GET, "/foo");

    // Error response from SERVER1, then a stalled future that will never resolve.
    stubLocatorCall(locations(SERVER1, SERVER2));
    expectHttpCall(requestBuilder, SERVER1)
        .thenReturn(errorResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, null, "oh no"))
        .thenReturn(SettableFuture.create());

    serviceClient = makeServiceClient(StandardRetryPolicy.unlimited());
    final ListenableFuture<Map<String, String>> response = doAsyncRequest(serviceClient, requestBuilder);

    Assert.assertTrue(response.cancel(true));
    Assert.assertTrue(response.isCancelled());
  }

  @Test
  public void test_computeBackoffMs()
  {
    final StandardRetryPolicy retryPolicy = StandardRetryPolicy.unlimited();

    Assert.assertEquals(100, ServiceClientImpl.computeBackoffMs(retryPolicy, 0));
    Assert.assertEquals(200, ServiceClientImpl.computeBackoffMs(retryPolicy, 1));
    Assert.assertEquals(3200, ServiceClientImpl.computeBackoffMs(retryPolicy, 5));
    Assert.assertEquals(30000, ServiceClientImpl.computeBackoffMs(retryPolicy, 20));
  }

  @Test
  public void test_serviceLocationNoPathFromUri()
  {
    Assert.assertNull(ServiceClientImpl.serviceLocationNoPathFromUri("/"));

    Assert.assertEquals(
        new ServiceLocation("1.2.3.4", 9999, -1, ""),
        ServiceClientImpl.serviceLocationNoPathFromUri("http://1.2.3.4:9999/foo")
    );

    Assert.assertEquals(
        new ServiceLocation("1.2.3.4", 80, -1, ""),
        ServiceClientImpl.serviceLocationNoPathFromUri("http://1.2.3.4/foo")
    );

    Assert.assertEquals(
        new ServiceLocation("1.2.3.4", -1, 9999, ""),
        ServiceClientImpl.serviceLocationNoPathFromUri("https://1.2.3.4:9999/foo")
    );

    Assert.assertEquals(
        new ServiceLocation("1.2.3.4", -1, 443, ""),
        ServiceClientImpl.serviceLocationNoPathFromUri("https://1.2.3.4/foo")
    );
  }

  @Test
  public void test_isRedirect()
  {
    Assert.assertTrue(ServiceClientImpl.isRedirect(HttpResponseStatus.FOUND));
    Assert.assertTrue(ServiceClientImpl.isRedirect(HttpResponseStatus.MOVED_PERMANENTLY));
    Assert.assertTrue(ServiceClientImpl.isRedirect(HttpResponseStatus.TEMPORARY_REDIRECT));
    Assert.assertFalse(ServiceClientImpl.isRedirect(HttpResponseStatus.OK));
  }

  private <T> OngoingStubbing<ListenableFuture<Either<StringFullResponseHolder, T>>> expectHttpCall(
      final RequestBuilder requestBuilder,
      final ServiceLocation location
  )
  {
    final Request expectedRequest = requestBuilder.build(location);

    return Mockito.when(
        httpClient.go(
            ArgumentMatchers.argThat(
                request ->
                    request != null
                    && expectedRequest.getMethod().equals(request.getMethod())
                    && expectedRequest.getUrl().equals(request.getUrl())
            ),
            ArgumentMatchers.any(ObjectOrErrorResponseHandler.class),
            ArgumentMatchers.eq(RequestBuilder.DEFAULT_TIMEOUT)
        )
    );
  }

  private void stubLocatorCall(final ServiceLocations locations)
  {
    stubLocatorCall(Futures.immediateFuture(locations));
  }

  private void stubLocatorCall(final ListenableFuture<ServiceLocations> locations)
  {
    Mockito.doReturn(locations).when(serviceLocator).locate();
  }

  private ServiceClient makeServiceClient(final ServiceRetryPolicy retryPolicy)
  {
    return new ServiceClientImpl(SERVICE_NAME, httpClient, serviceLocator, retryPolicy, exec);
  }

  private static Map<String, String> doRequest(
      final ServiceClient serviceClient,
      final RequestBuilder requestBuilder
  ) throws InterruptedException, ExecutionException
  {
    return serviceClient.request(requestBuilder, null /* Not verified by mocks */);
  }

  private static ListenableFuture<Map<String, String>> doAsyncRequest(
      final ServiceClient serviceClient,
      final RequestBuilder requestBuilder
  )
  {
    return serviceClient.asyncRequest(requestBuilder, null /* Not verified by mocks */);
  }

  private static <T> ListenableFuture<Either<StringFullResponseHolder, T>> valueResponse(final T o)
  {
    return Futures.immediateFuture(Either.value(o));
  }

  private static <T> ListenableFuture<Either<StringFullResponseHolder, T>> errorResponse(
      final HttpResponseStatus responseStatus,
      @Nullable final Map<String, String> headers,
      @Nullable final String content
  )
  {
    final DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, responseStatus);

    if (headers != null) {
      for (final Map.Entry<String, String> headerEntry : headers.entrySet()) {
        response.headers().add(headerEntry.getKey(), headerEntry.getValue());
      }
    }

    if (content != null) {
      response.setContent(ChannelBuffers.wrappedBuffer(ByteBuffer.wrap(StringUtils.toUtf8(content))));
    }

    final StringFullResponseHolder errorHolder = new StringFullResponseHolder(response, StandardCharsets.UTF_8);
    return Futures.immediateFuture(Either.error(errorHolder));
  }

  private static <T> ListenableFuture<Either<StringFullResponseHolder, T>> redirectResponse(final String newLocation)
  {
    return errorResponse(
        HttpResponseStatus.TEMPORARY_REDIRECT,
        ImmutableMap.of("location", newLocation),
        null
    );
  }

  private static ServiceLocations locations(final ServiceLocation... locations)
  {
    // ImmutableSet retains order, which is important.
    return ServiceLocations.forLocations(ImmutableSet.copyOf(locations));
  }
}
