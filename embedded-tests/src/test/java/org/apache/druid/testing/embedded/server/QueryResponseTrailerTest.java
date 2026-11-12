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

package org.apache.druid.testing.embedded.server;

import org.apache.druid.server.QueryResource;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedDruidServer;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.InputStream;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Verifies that streamed query responses carry the {@link QueryResource#RESPONSE_COMPLETE_TRAILER_HEADER} trailer only
 * when the client sends {@code TE: trailers}, so that clients which reject trailers, such as
 * {@link java.net.http.HttpClient} on recent JDK updates, can read query results.
 */
public class QueryResponseTrailerTest extends EmbeddedClusterTestBase
{
  private static final String SQL_QUERY = "{\"query\": \"SELECT 1\"}";
  private static final String NATIVE_QUERY = """
      {
        "queryType": "timeseries",
        "dataSource": "missing_datasource",
        "granularity": "all",
        "intervals": ["2000/3000"],
        "aggregations": [{"type": "count", "name": "rows"}]
      }
      """;

  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final HttpClient client = HttpClient.newHttpClient();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .addServer(coordinator)
                               .addServer(overlord)
                               .addServer(broker)
                               .addServer(historical);
  }

  @AfterAll
  public void closeClient()
  {
    client.close();
  }

  @Test
  @Timeout(30)
  public void testTrailersOnlyWhenClientSendsTeTrailers() throws Exception
  {
    for (final String path : new String[]{"/druid/v2/sql", "/druid/v2"}) {
      final String body = path.endsWith("sql") ? SQL_QUERY : NATIVE_QUERY;

      final String withoutTe = sendRaw(broker, path, body, false);
      Assertions.assertTrue(withoutTe.startsWith("HTTP/1.1 200"), withoutTe);
      Assertions.assertFalse(withoutTe.contains(QueryResource.RESPONSE_COMPLETE_TRAILER_HEADER), withoutTe);

      final String withTe = sendRaw(broker, path, body, true);
      Assertions.assertTrue(withTe.startsWith("HTTP/1.1 200"), withTe);
      Assertions.assertTrue(
          withTe.endsWith("\r\n0\r\n" + QueryResource.RESPONSE_COMPLETE_TRAILER_HEADER + ": true\r\n\r\n"),
          withTe
      );
    }
  }

  @Test
  @Timeout(30)
  public void testJavaHttpClientReadsStreamedResults() throws Exception
  {
    assertJavaHttpClientReadsResults(broker, "/druid/v2/sql", SQL_QUERY);
    assertJavaHttpClientReadsResults(broker, "/druid/v2", NATIVE_QUERY);
    assertJavaHttpClientReadsResults(historical, "/druid/v2", NATIVE_QUERY);
  }

  private void assertJavaHttpClientReadsResults(
      final EmbeddedDruidServer<?> server,
      final String path,
      final String body
  ) throws Exception
  {
    final HttpRequest request = HttpRequest.newBuilder(URI.create(getServerUrl(server) + path))
                                           .header("Content-Type", "application/json")
                                           .timeout(Duration.ofSeconds(10))
                                           .POST(HttpRequest.BodyPublishers.ofString(body))
                                           .build();
    final HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
    Assertions.assertEquals(200, response.statusCode());
    try (InputStream in = response.body()) {
      Assertions.assertTrue(new String(in.readAllBytes(), StandardCharsets.UTF_8).startsWith("["));
    }
  }

  /**
   * Sends a request over a raw keep-alive socket and returns the response bytes as they appear on the wire, including
   * chunk framing and trailers, which {@link HttpClient} does not expose.
   */
  private String sendRaw(
      final EmbeddedDruidServer<?> server,
      final String path,
      final String body,
      final boolean teTrailers
  ) throws Exception
  {
    final URI uri = URI.create(getServerUrl(server));
    final byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
    final String request = "POST " + path + " HTTP/1.1\r\n"
                           + "Host: " + uri.getHost() + "\r\n"
                           + "Content-Type: application/json\r\n"
                           + "Content-Length: " + bodyBytes.length + "\r\n"
                           + (teTrailers ? "TE: trailers\r\nConnection: TE\r\n" : "")
                           + "\r\n";
    try (Socket socket = new Socket(uri.getHost(), uri.getPort())) {
      socket.setSoTimeout(10_000);
      socket.getOutputStream().write(request.getBytes(StandardCharsets.US_ASCII));
      socket.getOutputStream().write(bodyBytes);
      socket.getOutputStream().flush();

      final InputStream in = socket.getInputStream();
      final StringBuilder response = new StringBuilder();
      while (!isComplete(response)) {
        final int b = in.read();
        if (b < 0) {
          break;
        }
        response.append((char) b);
      }
      return response.toString();
    }
  }

  private static boolean isComplete(final CharSequence response)
  {
    final String text = response.toString();
    final int headersEnd = text.indexOf("\r\n\r\n");
    if (headersEnd < 0) {
      return false;
    }
    final String headers = text.substring(0, headersEnd).toLowerCase(Locale.ROOT);
    final Matcher contentLength = Pattern.compile("\r\ncontent-length: *(\\d+)").matcher(headers);
    if (contentLength.find()) {
      return text.length() >= headersEnd + 4 + Integer.parseInt(contentLength.group(1));
    }
    // Chunked: complete once the last chunk and the (possibly empty) trailer section have been read.
    final int lastChunk = text.indexOf("\r\n0\r\n", headersEnd);
    return lastChunk >= 0 && text.endsWith("\r\n\r\n") && text.length() >= lastChunk + 7;
  }
}
