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

package org.apache.druid.data.input.impl;

import com.google.common.net.HttpHeaders;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.AdditionalAnswers;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;

public class HttpEntityTest
{
  private URI uri;
  private URL url;
  private URLConnection urlConnection;
  private InputStream inputStreamMock;

  @Before
  public void setup() throws IOException
  {
    uri = Mockito.mock(URI.class);
    url = Mockito.mock(URL.class);
    urlConnection = Mockito.mock(URLConnection.class);
    inputStreamMock = Mockito.mock(InputStream.class);
    Mockito.when(uri.toURL()).thenReturn(url);
    Mockito.when(url.openConnection()).thenReturn(urlConnection);
    Mockito.when(urlConnection.getInputStream()).thenReturn(inputStreamMock);
    Mockito.when(inputStreamMock.skip(ArgumentMatchers.anyLong())).then(AdditionalAnswers.returnsFirstArg());
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testOpenInputStream() throws IOException, URISyntaxException
  {
    HttpServer server = null;
    InputStream inputStream = null;
    InputStream inputStreamPartial = null;
    ServerSocket serverSocket = null;
    try {
      serverSocket = new ServerSocket(0);
      int port = serverSocket.getLocalPort();
      // closing port so that the httpserver can use. Can cause race conditions.
      serverSocket.close();
      server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
      server.createContext(
          "/test",
          (httpExchange) -> {
            String payload = "12345678910";
            byte[] outputBytes = payload.getBytes(StandardCharsets.UTF_8);
            httpExchange.sendResponseHeaders(200, outputBytes.length);
            OutputStream os = httpExchange.getResponseBody();
            httpExchange.getResponseHeaders().set(HttpHeaders.CONTENT_TYPE, "application/octet-stream");
            httpExchange.getResponseHeaders().set(HttpHeaders.CONTENT_LENGTH, String.valueOf(outputBytes.length));
            httpExchange.getResponseHeaders().set(HttpHeaders.CONTENT_RANGE, "bytes 0");
            os.write(outputBytes);
            os.close();
          }
      );
      server.start();

      URI url = new URI("http://" + server.getAddress().getHostName() + ":" + server.getAddress().getPort() + "/test");
      inputStream = HttpEntity.openInputStream(url, "", null, 0);
      inputStreamPartial = HttpEntity.openInputStream(url, "", null, 5);
      inputStream.skip(5);
      Assert.assertTrue(IOUtils.contentEquals(inputStream, inputStreamPartial));
    }
    finally {
      IOUtils.closeQuietly(inputStream);
      IOUtils.closeQuietly(inputStreamPartial);
      if (server != null) {
        server.stop(0);
      }
      if (serverSocket != null) {
        serverSocket.close();
      }
    }
  }

  @Test
  public void testWithServerSupportingRanges() throws IOException
  {
    long offset = 15;
    String contentRange = StringUtils.format("bytes %d-%d/%d", offset, 1000, 1000);
    Mockito.when(urlConnection.getHeaderField(HttpHeaders.CONTENT_RANGE)).thenReturn(contentRange);
    HttpEntity.openInputStream(uri, "", null, offset);
    Mockito.verify(inputStreamMock, Mockito.times(0)).skip(offset);
  }

  @Test
  public void testWithServerNotSupportingRanges() throws IOException
  {
    long offset = 15;
    Mockito.when(urlConnection.getHeaderField(HttpHeaders.CONTENT_RANGE)).thenReturn(null);
    HttpEntity.openInputStream(uri, "", null, offset);
    Mockito.verify(inputStreamMock, Mockito.times(1)).skip(offset);
  }

  @Test
  public void testWithServerNotSupportingBytesRanges() throws IOException
  {
    long offset = 15;
    Mockito.when(urlConnection.getHeaderField(HttpHeaders.CONTENT_RANGE)).thenReturn("token 2-12/12");
    HttpEntity.openInputStream(uri, "", null, offset);
    Mockito.verify(inputStreamMock, Mockito.times(1)).skip(offset);
  }
}
