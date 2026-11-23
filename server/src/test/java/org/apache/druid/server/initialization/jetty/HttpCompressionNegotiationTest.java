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


package org.apache.druid.server.initialization.jetty;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStream;
import com.google.common.collect.ImmutableList;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.URIs;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.HttpClientConfig;
import org.apache.druid.java.util.http.client.HttpClientConfig.CompressionCodec;
import org.apache.druid.java.util.http.client.HttpClientInit;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.server.initialization.HttpServerCompression;
import org.eclipse.jetty.ee8.servlet.ServletContextHandler;
import org.eclipse.jetty.ee8.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Tests response encoding negotiation between Druid's HTTP client and the handler built by
 * {@link JettyServerInitUtils#wrapWithCompressionHandler}, for every combination of old and new client and
 * server settings.
 */
public class HttpCompressionNegotiationTest
{
  private static final String ECHO_ACCEPT_ENCODING_HEADER = "X-Echo-Accept-Encoding";
  private static final byte[] PAYLOAD = makePayload(2_000_000);

  /**
   * What an old client sends, and what a new client sends by default.
   */
  private static final String OLD_CLIENT_ACCEPT_ENCODING = "gzip";
  private static final String NEW_CLIENT_ACCEPT_ENCODING = "zstd, gzip;q=0.5";

  private static final HttpServerCompression OLD_SERVER = config(ImmutableList.of("gzip"));
  private static final HttpServerCompression NEW_SERVER = HttpServerCompression.DEFAULT;

  private final List<Server> servers = new ArrayList<>();
  private final List<Lifecycle> lifecycles = new ArrayList<>();

  @AfterEach
  public void tearDown() throws Exception
  {
    for (Lifecycle lifecycle : lifecycles) {
      lifecycle.stop();
    }
    for (Server server : servers) {
      server.stop();
    }
  }

  @Test
  public void testRollingUpgradeMatrix() throws Exception
  {
    final URL oldServer = startServer(OLD_SERVER);
    final URL newServer = startServer(NEW_SERVER);

    assertResponseEncoding(oldServer, OLD_CLIENT_ACCEPT_ENCODING, "gzip");
    assertResponseEncoding(newServer, OLD_CLIENT_ACCEPT_ENCODING, "gzip");
    assertResponseEncoding(oldServer, NEW_CLIENT_ACCEPT_ENCODING, "gzip");
    assertResponseEncoding(newServer, NEW_CLIENT_ACCEPT_ENCODING, "zstd");
  }

  @Test
  public void testServerPreferenceWins() throws Exception
  {
    // Among the encodings the client accepts, the server picks its own most preferred one.
    assertResponseEncoding(startServer(NEW_SERVER), "gzip, zstd;q=0.5", "zstd");
    assertResponseEncoding(startServer(config(ImmutableList.of("gzip", "zstd"))), "zstd, gzip;q=0.5", "gzip");
  }

  @Test
  public void testNoCompression() throws Exception
  {
    final URL newServer = startServer(NEW_SERVER);
    assertResponseEncoding(newServer, null, null);
    assertResponseEncoding(newServer, "identity", null);
    assertResponseEncoding(newServer, "br", null);
    assertResponseEncoding(startServer(config(ImmutableList.of())), NEW_CLIENT_ACCEPT_ENCODING, null);
    assertResponseEncoding(startServer(config(ImmutableList.of("zstd"))), OLD_CLIENT_ACCEPT_ENCODING, null);
  }

  @Test
  public void testWildcardDoesNotPickDeclinedCodec() throws Exception
  {
    final URL newServer = startServer(NEW_SERVER);
    assertResponseEncoding(newServer, "*", "zstd");
    assertResponseEncoding(newServer, "zstd;q=0, *;q=1", "gzip");
    assertResponseEncoding(newServer, "ZSTD;q=0, *", "gzip");
    assertResponseEncoding(newServer, "zstd;q=0, gzip;q=0, *", null);
    assertResponseEncoding(startServer(config(ImmutableList.of("zstd"))), "zstd;q=0, *", null);
  }

  @Test
  public void testCodecNamesIgnoreCase() throws Exception
  {
    assertResponseEncoding(startServer(NEW_SERVER), "ZSTD, GZIP;q=0.5", "zstd");
    assertResponseEncoding(startServer(OLD_SERVER), "Gzip", "gzip");
    assertResponseEncoding(startServer(config(ImmutableList.of())), "br, GZIP", null);
    assertResponseEncoding(startServer(config(ImmutableList.of("zstd"))), "br, GZIP", null);
  }

  @Test
  public void testHandlerSeesOriginalAcceptEncoding() throws Exception
  {
    final HttpURLConnection connection = (HttpURLConnection) startServer(NEW_SERVER).openConnection();
    connection.setRequestProperty("Accept-Encoding", "ZSTD;q=0, *");
    try (InputStream in = connection.getInputStream()) {
      decode(connection.getContentEncoding(), in);
    }
    Assertions.assertEquals("ZSTD;q=0, *", connection.getHeaderField(ECHO_ACCEPT_ENCODING_HEADER));
  }

  @Test
  public void testNormalizeAcceptEncoding()
  {
    final List<String> codecs = ImmutableList.of("zstd", "gzip");
    Assertions.assertEquals("zstd, gzip;q=0.5", normalize("zstd, gzip;q=0.5", codecs));
    Assertions.assertEquals("zstd, gzip", normalize("*", codecs));
    Assertions.assertEquals("zstd;q=0.0, gzip;q=0.5", normalize("ZSTD;q=0, *;q=0.5", codecs));
    Assertions.assertEquals("br, gzip", normalize("BR, GZIP", codecs));
    Assertions.assertEquals("identity", normalize("identity, *", ImmutableList.of()));
    Assertions.assertNull(normalize("*", ImmutableList.of()));
  }

  @Nullable
  private static String normalize(final String acceptEncoding, final List<String> codecs)
  {
    return NormalizeAcceptEncodingHandler.normalizeAcceptEncoding(ImmutableList.of(acceptEncoding), codecs);
  }

  @Test
  public void testPostResponseIsCompressed() throws Exception
  {
    final URL newServer = startServer(NEW_SERVER);
    final HttpURLConnection connection = (HttpURLConnection) newServer.openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Accept-Encoding", NEW_CLIENT_ACCEPT_ENCODING);
    connection.setDoOutput(true);
    connection.getOutputStream().write(PAYLOAD);
    Assertions.assertEquals("zstd", connection.getContentEncoding());
    Assertions.assertArrayEquals(PAYLOAD, decode("zstd", connection.getInputStream()));
  }

  @Test
  public void testCompressionLevels() throws Exception
  {
    final int zstdFastSize = responseSize(startServer(zstdLevel(1)), "zstd");
    final int zstdSlowSize = responseSize(startServer(zstdLevel(19)), "zstd");
    Assertions.assertTrue(
        zstdSlowSize < zstdFastSize,
        StringUtils.format("zstd level 19 size[%d], level 1 size[%d]", zstdSlowSize, zstdFastSize)
    );

    // Level 0 stores the data without compressing it.
    final int gzipStoredSize = responseSize(startServer(gzipLevel(0)), "gzip");
    final int gzipDefaultSize = responseSize(startServer(NEW_SERVER), "gzip");
    Assertions.assertTrue(gzipStoredSize > PAYLOAD.length, "gzip level 0 size: " + gzipStoredSize);
    Assertions.assertTrue(gzipDefaultSize < PAYLOAD.length / 2, "gzip default size: " + gzipDefaultSize);
  }

  @Test
  public void testDruidHttpClient() throws Exception
  {
    final URL oldServer = startServer(OLD_SERVER);
    final URL newServer = startServer(NEW_SERVER);
    final HttpClient oldClient = makeClient(ImmutableList.of(CompressionCodec.GZIP));
    final HttpClient newClient = makeClient(null);
    final HttpClient identityClient = makeClient(ImmutableList.of());

    for (URL server : ImmutableList.of(oldServer, newServer)) {
      assertDruidClientResponse(oldClient, server, null, OLD_CLIENT_ACCEPT_ENCODING);
      assertDruidClientResponse(newClient, server, null, NEW_CLIENT_ACCEPT_ENCODING);
      assertDruidClientResponse(identityClient, server, null, "identity");
    }

    // An Accept-Encoding header set on the request overrides the client's default. Asking for zstd alone forces a zstd
    // response, which the client must decode.
    assertDruidClientResponse(oldClient, newServer, "zstd", "zstd");
    assertDruidClientResponse(newClient, newServer, "gzip", "gzip");
  }

  @Test
  public void testDruidHttpClientDecodesHighestZstdLevel() throws Exception
  {
    // Level 19 uses the largest window of the allowed levels, so this checks that the client's zstd decoder accepts it.
    final URL server = startServer(zstdLevel(HttpServerCompression.Zstd.MAX_LEVEL));
    assertDruidClientResponse(makeClient(null), server, null, NEW_CLIENT_ACCEPT_ENCODING);
  }

  @Test
  public void testRequestDecompression() throws Exception
  {
    final URL newServer = startServer(NEW_SERVER);

    final byte[] gzipped = gzip(PAYLOAD);
    Assertions.assertArrayEquals(PAYLOAD, postAndReadEcho(newServer, "gzip", gzipped));
    Assertions.assertArrayEquals(PAYLOAD, postAndReadEcho(newServer, "zstd", Zstd.compress(PAYLOAD, 3)));

    // gzip request bodies are decompressed even when gzip is disabled for responses.
    final URL compressionOffServer = startServer(config(ImmutableList.of()));
    Assertions.assertArrayEquals(PAYLOAD, postAndReadEcho(compressionOffServer, "gzip", gzipped));

    // zstd is not registered at all when disabled, so zstd request bodies pass through as is.
    final URL gzipOnlyServer = startServer(OLD_SERVER);
    final byte[] zstdBody = Zstd.compress(PAYLOAD, 3);
    Assertions.assertArrayEquals(zstdBody, postAndReadEcho(gzipOnlyServer, "zstd", zstdBody));
  }

  @Test
  public void testRequestDecompressionOfAnyMethod() throws Exception
  {
    // As with the old GzipHandler, request bodies are decompressed for every method, not only POST.
    final URL newServer = startServer(NEW_SERVER);
    final HttpClient client = makeClient(null);
    for (String method : ImmutableList.of("GET", "PUT", "DELETE", "PATCH", "OPTIONS")) {
      for (String encoding : ImmutableList.of("gzip", "zstd")) {
        final byte[] body = "gzip".equals(encoding) ? gzip(PAYLOAD) : Zstd.compress(PAYLOAD, 3);
        Assertions.assertArrayEquals(
            PAYLOAD,
            sendAndReadEcho(client, newServer, method, encoding, body),
            StringUtils.format("%s %s", method, encoding)
        );
      }
    }
  }

  @Test
  public void testRequestDecompressionOfAllowedCustomMethod() throws Exception
  {
    // A method that Jetty does not know is decompressed only if druid.server.http.allowedHttpMethods allows it.
    // Otherwise AllowHttpMethodsResourceFilter rejects it, so it is not added here and its body passes through as is.
    final String method = "X-DRUID-CUSTOM";
    final byte[] body = Zstd.compress(PAYLOAD, 3);
    final HttpClient client = makeClient(null);

    final URL allowedServer = startServer(NEW_SERVER, List.of(method));
    Assertions.assertArrayEquals(PAYLOAD, sendAndReadEcho(client, allowedServer, method, "zstd", body));

    final URL notAllowedServer = startServer(NEW_SERVER);
    Assertions.assertArrayEquals(body, sendAndReadEcho(client, notAllowedServer, method, "zstd", body));
  }

  @Test
  public void testZeroInflateBufferSizeDisablesRequestDecompression() throws Exception
  {
    // As with the old GzipHandler, an inflateBufferSize of 0 disables request decompression, for every codec.
    // Compressed bodies reach the servlet as is.
    final URL server = startServer(
        new HttpServerCompression(null, null, new HttpServerCompression.Gzip(null, 0))
    );
    final byte[] gzipBody = gzip(PAYLOAD);
    final byte[] zstdBody = Zstd.compress(PAYLOAD, 3);
    Assertions.assertArrayEquals(gzipBody, postAndReadEcho(server, "gzip", gzipBody));
    Assertions.assertArrayEquals(zstdBody, postAndReadEcho(server, "zstd", zstdBody));
  }

  private static byte[] gzip(final byte[] bytes) throws IOException
  {
    final ByteArrayOutputStream gzipped = new ByteArrayOutputStream();
    try (GZIPOutputStream out = new GZIPOutputStream(gzipped)) {
      out.write(bytes);
    }
    return gzipped.toByteArray();
  }

  private void assertResponseEncoding(
      final URL url,
      @Nullable final String acceptEncoding,
      @Nullable final String expectedEncoding
  ) throws IOException
  {
    final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    if (acceptEncoding != null) {
      connection.setRequestProperty("Accept-Encoding", acceptEncoding);
    }
    Assertions.assertEquals(200, connection.getResponseCode());
    Assertions.assertEquals(
        expectedEncoding,
        connection.getContentEncoding(),
        StringUtils.format("Accept-Encoding[%s] to [%s]", acceptEncoding, url)
    );
    Assertions.assertArrayEquals(PAYLOAD, decode(expectedEncoding, connection.getInputStream()));
  }

  private void assertDruidClientResponse(
      final HttpClient client,
      final URL url,
      @Nullable final String requestAcceptEncoding,
      final String expectedAcceptEncoding
  ) throws Exception
  {
    final Request request = new Request(HttpMethod.GET, url);
    if (requestAcceptEncoding != null) {
      request.setHeader("Accept-Encoding", requestAcceptEncoding);
    }
    final BytesFullResponseHolder holder = client.go(request, new BytesFullResponseHandler()).get();
    Assertions.assertEquals(200, holder.getStatus().code());
    Assertions.assertEquals(expectedAcceptEncoding, holder.getResponse().headers().get(ECHO_ACCEPT_ENCODING_HEADER));
    // The client decodes the response, and removes the Content-Encoding header after doing so.
    Assertions.assertNull(holder.getResponse().headers().get("Content-Encoding"));
    Assertions.assertArrayEquals(PAYLOAD, holder.getContent());
  }

  private int responseSize(final URL url, final String encoding) throws IOException
  {
    final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestProperty("Accept-Encoding", encoding);
    Assertions.assertEquals(encoding, connection.getContentEncoding());
    final byte[] compressed = IOUtils.toByteArray(connection.getInputStream());
    Assertions.assertArrayEquals(PAYLOAD, decode(encoding, new ByteArrayInputStream(compressed)));
    return compressed.length;
  }

  private static byte[] sendAndReadEcho(
      final HttpClient client,
      final URL url,
      final String method,
      final String contentEncoding,
      final byte[] body
  ) throws Exception
  {
    final Request request = new Request(HttpMethod.valueOf(method), url)
        .setHeader("Content-Encoding", contentEncoding)
        .setContent("application/octet-stream", body);
    final BytesFullResponseHolder holder = client.go(request, new BytesFullResponseHandler()).get();
    Assertions.assertEquals(200, holder.getStatus().code(), method);
    return holder.getContent();
  }

  private byte[] postAndReadEcho(final URL url, final String contentEncoding, final byte[] body) throws IOException
  {
    final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Content-Encoding", contentEncoding);
    connection.setRequestProperty("Content-Type", "application/octet-stream");
    connection.setDoOutput(true);
    connection.getOutputStream().write(body);
    Assertions.assertEquals(200, connection.getResponseCode());
    Assertions.assertNull(connection.getContentEncoding());
    return IOUtils.toByteArray(connection.getInputStream());
  }

  /**
   * Creates a Druid HTTP client that accepts the given codecs, or the client's default codecs if null.
   */
  private HttpClient makeClient(@Nullable final List<CompressionCodec> codecs) throws Exception
  {
    final Lifecycle lifecycle = new Lifecycle();
    lifecycles.add(lifecycle);
    final HttpClientConfig.Builder builder = HttpClientConfig.builder();
    if (codecs != null) {
      builder.withCompressionCodecs(codecs);
    }
    final HttpClient client = HttpClientInit.createClient(builder.build(), lifecycle);
    lifecycle.start();
    return client;
  }

  private URL startServer(final HttpServerCompression compressionConfig) throws Exception
  {
    return startServer(compressionConfig, List.of());
  }

  private URL startServer(final HttpServerCompression compressionConfig, final List<String> allowedHttpMethods)
      throws Exception
  {
    final Server server = new Server();
    final ServerConnector connector = new ServerConnector(server);
    connector.setPort(0);
    server.addConnector(connector);

    final ServletContextHandler root = new ServletContextHandler();
    root.addServlet(new ServletHolder(new PayloadServlet()), "/*");
    server.setHandler(
        JettyServerInitUtils.wrapWithCompressionHandler(root, compressionConfig, allowedHttpMethods)
    );
    servers.add(server);
    server.start();
    return URIs.toURL(StringUtils.format("http://localhost:%d/payload", connector.getLocalPort()));
  }

  private static byte[] decode(@Nullable final String encoding, final InputStream in) throws IOException
  {
    if (encoding == null) {
      return IOUtils.toByteArray(in);
    }
    switch (encoding) {
      case "gzip":
        return IOUtils.toByteArray(new GZIPInputStream(in));
      case "zstd":
        return IOUtils.toByteArray(new ZstdInputStream(in));
      default:
        throw new IllegalArgumentException(encoding);
    }
  }

  private static HttpServerCompression config(final List<String> codecs)
  {
    return new HttpServerCompression(codecs, null, null);
  }

  private static HttpServerCompression zstdLevel(final int level)
  {
    return new HttpServerCompression(ImmutableList.of("zstd"), new HttpServerCompression.Zstd(level), null);
  }

  private static HttpServerCompression gzipLevel(final int level)
  {
    return new HttpServerCompression(ImmutableList.of("gzip"), null, new HttpServerCompression.Gzip(level, null));
  }

  private static byte[] makePayload(final int size)
  {
    // Repetitive structure with random values, like a JSON list of segments, so that higher compression levels give
    // smaller output.
    final Random random = new Random(42);
    final StringBuilder builder = new StringBuilder(size + 200);
    while (builder.length() < size) {
      builder.append("{\"dataSource\":\"wikipedia\",\"interval\":\"2026-01-")
             .append(1 + random.nextInt(28))
             .append("\",\"version\":\"")
             .append(Long.toHexString(random.nextLong()))
             .append("\",\"partition\":")
             .append(random.nextInt(1000))
             .append(",\"size\":")
             .append(random.nextInt(Integer.MAX_VALUE))
             .append("},");
    }
    return StringUtils.toUtf8(builder.substring(0, size));
  }

  /**
   * Echoes the request body, or returns {@link #PAYLOAD} if the request has no body. Also echoes the request's
   * {@code Accept-Encoding} header, so tests can see what the client sent.
   */
  private static class PayloadServlet extends HttpServlet
  {
    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
      final byte[] body = IOUtils.toByteArray(req.getInputStream());
      write(req, resp, body.length == 0 ? PAYLOAD : body);
    }

    private static void write(HttpServletRequest req, HttpServletResponse resp, byte[] body) throws IOException
    {
      resp.setStatus(200);
      resp.setContentType("application/json");
      final String acceptEncoding = req.getHeader("Accept-Encoding");
      if (acceptEncoding != null) {
        resp.setHeader(ECHO_ACCEPT_ENCODING_HEADER, acceptEncoding);
      }
      resp.getOutputStream().write(body);
    }
  }
}
