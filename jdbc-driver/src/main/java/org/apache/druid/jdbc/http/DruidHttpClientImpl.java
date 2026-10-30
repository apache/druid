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

package org.apache.druid.jdbc.http;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jdbc.ClientProperties;
import org.apache.druid.jdbc.DruidConnectionUrl;
import org.apache.druid.jdbc.DruidJdbcException;
import org.apache.druid.jdbc.DruidSQLState;
import org.apache.druid.jdbc.StringUtils;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Base64;
import java.util.Locale;

/**
 * Production implementation of {@link DruidHttpClient}, using the JDK {@link HttpClient}.
 */
public class DruidHttpClientImpl implements DruidHttpClient
{
  private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration CANCELLATION_TIMEOUT = Duration.ofSeconds(5);
  private static final String CTX_SQL_QUERY_ID = "sqlQueryId";
  private static final String CONTENT_TYPE_JSON = "application/json";

  private final HttpClient httpClient;
  private final ObjectMapper jsonMapper;
  private final String httpUrl;
  private final ClientProperties clientProperties;

  private volatile int networkTimeoutMillis;
  private volatile boolean closed;

  public DruidHttpClientImpl(
      final DruidConnectionUrl connectionUrl,
      final ObjectMapper jsonMapper
  ) throws SQLException
  {
    this.httpUrl = connectionUrl.buildHttpUrl();
    this.clientProperties = connectionUrl.getClientProperties();

    final HttpClient.Builder clientBuilder =
        HttpClient.newBuilder()
                  .connectTimeout(DEFAULT_CONNECT_TIMEOUT)
                  .followRedirects(HttpClient.Redirect.NEVER);

    if (connectionUrl.isHttps() && !clientProperties.isVerifyTls()) {
      try {
        final SSLContext sslContext = createTrustAllSslContext();
        clientBuilder.sslContext(sslContext);
      }
      catch (Exception e) {
        throw new DruidJdbcException(e, "Failed to configure TLS context: %s", e);
      }
    }

    this.httpClient = clientBuilder.build();
    this.jsonMapper = jsonMapper;
  }

  @Override
  public QueryResultsIterator runQuery(final SqlRequest request) throws SQLException
  {
    throwIfClosed();

    try {
      final byte[] requestJson = jsonMapper.writeValueAsBytes(request);
      final HttpRequest.Builder requestBuilder =
          createRequestBuilder(URI.create(httpUrl))
              .header("Content-Type", CONTENT_TYPE_JSON)
              .POST(HttpRequest.BodyPublishers.ofByteArray(requestJson));

      final String sqlQueryId = sqlQueryIdOf(request);
      final HttpResponse<InputStream> response = executeQueryRequest(requestBuilder);
      try {
        return new QueryResultsIteratorImpl(response.body(), jsonMapper, sqlQueryId);
      }
      catch (Throwable e) {
        try {
          response.body().close();
        }
        catch (Throwable e2) {
          e.addSuppressed(e2);
        }
        throw e;
      }
    }
    catch (SQLException e) {
      throw e;
    }
    catch (Exception e) {
      throw new DruidJdbcException(e, "Failed to execute SQL query: %s", e);
    }
  }

  @Override
  public void cancelQuery(final String sqlQueryId) throws SQLException
  {
    throwIfClosed();

    if (sqlQueryId == null || sqlQueryId.isEmpty()) {
      throw new DruidJdbcException("sqlQueryId cannot be null or empty");
    }

    try {
      final String cancellationUrl =
          (httpUrl.endsWith("/") ? httpUrl.substring(0, httpUrl.length() - 1) : httpUrl)
          + "/"
          + encodePathComponent(sqlQueryId);

      final HttpRequest.Builder requestBuilder =
          createRequestBuilder(URI.create(cancellationUrl))
              .DELETE()
              .timeout(CANCELLATION_TIMEOUT);

      final HttpRequest request = requestBuilder.build();
      final HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());

      // Consider 404 or 2xx a success, any other return code a failure.
      final int statusCode = response.statusCode();
      if (!(statusCode >= 200 && statusCode < 300) && statusCode != 404) {
        throw new DruidJdbcException(
            "Failed to cancel sqlQueryId[%s]: Received HTTP %s", sqlQueryId, statusCode);
      }
    }
    catch (SQLException e) {
      throw e;
    }
    catch (Exception e) {
      throw new DruidJdbcException(e, "Failed to cancel sqlQueryId[%s]: %s", sqlQueryId, e);
    }
  }

  @Override
  public String getUrl()
  {
    return httpUrl;
  }

  @Override
  public int getNetworkTimeoutMillis()
  {
    return networkTimeoutMillis;
  }

  @Override
  public void setNetworkTimeoutMillis(final int networkTimeoutMillis)
  {
    this.networkTimeoutMillis = networkTimeoutMillis;
  }

  @Override
  public boolean isClosed()
  {
    return closed;
  }

  @Override
  public void close()
  {
    try {
      HttpClientUtils.close(httpClient);
    }
    catch (RuntimeException e) {
      throw e;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    finally {
      closed = true;
    }
  }

  /**
   * Executes an HTTP request, returning the response as a stream if it succeeds. Throws an error for non-successful
   * HTTP codes.
   *
   * @throws SQLException if the HTTP request fails
   */
  private HttpResponse<InputStream> executeQueryRequest(final HttpRequest.Builder requestBuilder) throws SQLException
  {
    throwIfClosed();

    try {
      // Apply network timeout, if set, to the HTTP connection.
      final int networkTimeoutMillisToUse = networkTimeoutMillis;
      if (networkTimeoutMillisToUse > 0) {
        requestBuilder.timeout(Duration.ofMillis(networkTimeoutMillisToUse));
      }

      final HttpRequest request = requestBuilder.build();
      final HttpResponse<InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());

      if (response.statusCode() >= 300) {
        // Handle errors. 3xx counts as an error (in addition to 4xx and 5xx), since we do not follow redirects.
        try (InputStream inputStream = response.body()) {
          final String errorBody = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
          throw toSQLException(response.statusCode(), response.headers(), errorBody);
        }
      } else {
        return response;
      }
    }
    catch (SQLException e) {
      throw e;
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DruidJdbcException(e, "Failed to execute query against[%s]: interrupted: %s", httpUrl, e);
    }
    catch (ConnectException | UnknownHostException e) {
      throw new DruidJdbcException(
          e,
          DruidSQLState.ConnectionUnableToConnect,
          "Failed to connect to[%s]: %s",
          httpUrl,
          e
      );
    }
    catch (Exception e) {
      throw new DruidJdbcException(e, "Failed to execute query against[%s]: %s", httpUrl, e);
    }
  }

  /**
   * Creates an HTTP request builder with authentication headers added.
   */
  private HttpRequest.Builder createRequestBuilder(final URI uri) throws SQLException
  {
    throwIfClosed();

    final HttpRequest.Builder builder = HttpRequest.newBuilder().uri(uri).header("Accept", CONTENT_TYPE_JSON);

    if (clientProperties.getAuthentication() != null) {
      final String authentication = clientProperties.getAuthentication();

      if (ClientProperties.AUTHENTICATION_BASIC.equals(authentication)) {
        final String user = clientProperties.getUser();
        final String password = clientProperties.getPassword();
        final String credentials = (user == null ? "" : user) + ":" + (password == null ? "" : password);
        final String encodedCredentials =
            Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));

        builder.header("Authorization", "Basic " + encodedCredentials);
      } else if (ClientProperties.AUTHENTICATION_BASIC_RAW.equals(authentication)) {
        final String password = clientProperties.getPassword();

        if (password == null || password.isEmpty()) {
          throw new DruidJdbcException("Password is required for basicRaw authentication");
        }

        builder.header("Authorization", "Basic " + password);
      } else {
        throw new DruidJdbcException("Unsupported authentication method: %s", authentication);
      }
    }

    return builder;
  }

  SQLException toSQLException(final int statusCode, final HttpHeaders headers, @Nullable final String body)
  {
    if (statusCode >= 300 && statusCode < 400) {
      return new DruidJdbcException(
          "HTTP %s redirect from[%s] to[%s]: connect to the redirect target directly",
          statusCode,
          httpUrl,
          headers.firstValue("location").orElse("none")
      );
    }

    // Check if the response is JSON, try to parse the error response if so.
    String formattedError = null;

    if (body != null && !body.isEmpty() && isJsonContentType(headers)) {
      try {
        final ErrorResponse errorResponse = jsonMapper.readValue(body, ErrorResponse.class);
        formattedError = errorResponse.asFullErrorMessage();
      }
      catch (Exception ignored) {
        // Fall back to default error message.
      }
    }

    if (formattedError == null) {
      formattedError = chopBody(body);
    }

    if (statusCode == 401 || statusCode == 403) {
      return new DruidJdbcException(
          DruidSQLState.InvalidAuthorizationSpecification,
          "HTTP %s error from[%s]: %s",
          statusCode,
          httpUrl,
          formattedError
      );
    } else {
      return new DruidJdbcException("HTTP %s error from[%s]: %s", statusCode, httpUrl, formattedError);
    }
  }

  private void throwIfClosed() throws SQLException
  {
    if (closed) {
      throw new DruidJdbcException(DruidSQLState.ConnectionDoesNotExist, "HTTP client is closed");
    }
  }

  /**
   * Returns the sqlQueryId from a request's query context, or null if it has none. Normally set by the statement
   * that built the request, both for cancellation and so errors can name the query.
   */
  @Nullable
  private static String sqlQueryIdOf(final SqlRequest request)
  {
    final Object sqlQueryId = request.context().get(CTX_SQL_QUERY_ID);
    return sqlQueryId == null ? null : String.valueOf(sqlQueryId);
  }

  /**
   * Chops a string to 1000 chars or less. Meant for inclusion in error messages.
   */
  private static String chopBody(@Nullable final String body)
  {
    final int maxChars = 1000;

    if (body == null || body.isEmpty()) {
      return "no body";
    } else if (body.length() > maxChars) {
      return StringUtils.format("first 1K chars of body: %s", body.substring(0, maxChars));
    } else {
      return StringUtils.format("body: %s", body);
    }
  }

  /**
   * Encodes a URL path component.
   */
  @Nullable
  private static String encodePathComponent(@Nullable String s)
  {
    if (s == null) {
      return null;
    }

    return StringUtils.replace(URLEncoder.encode(s, StandardCharsets.UTF_8), "+", "%20");
  }

  /**
   * Creates an SSL context that trusts all certificates without verification.
   * This is only used when {@link ClientProperties#isVerifyTls()} is explicitly disabled.
   */
  private static SSLContext createTrustAllSslContext() throws Exception
  {
    final TrustManager[] trustAllCerts = new TrustManager[]{
        new X509TrustManager()
        {
          @Override
          public X509Certificate[] getAcceptedIssuers()
          {
            return new X509Certificate[0];
          }

          @Override
          public void checkClientTrusted(final X509Certificate[] certs, final String authType)
          {
            // Accept all client certificates
          }

          @Override
          public void checkServerTrusted(final X509Certificate[] certs, final String authType)
          {
            // Accept all server certificates
          }
        }
    };

    final SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(null, trustAllCerts, new SecureRandom());
    return sslContext;
  }

  /**
   * Checks if the HTTP response content type is JSON.
   */
  private static boolean isJsonContentType(final HttpHeaders headers)
  {
    return headers.firstValue("content-type")
                  .map(contentType -> contentType.toLowerCase(Locale.ENGLISH).contains(CONTENT_TYPE_JSON))
                  .orElse(false);
  }

  /**
   * Models an error response from the Druid SQL API.
   *
   * @param error        well-defined error code
   * @param errorMessage message with additional details about the error
   * @param errorClass   class of exception that caused this error
   * @param host         host on which the error occurred
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record ErrorResponse(
      @JsonProperty("error") String error,
      @JsonProperty("errorMessage") String errorMessage,
      @JsonProperty("errorClass") String errorClass,
      @JsonProperty("host") String host
  )
  {
    @Nullable
    public String asFullErrorMessage()
    {
      if (error == null) {
        return null;
      }

      final StringBuilder errorBuilder = new StringBuilder();
      errorBuilder.append(error);

      // Add host if available
      if (host != null) {
        errorBuilder.append(" from[").append(host).append("]");
      }

      // Add error class if available
      if (errorClass != null) {
        errorBuilder.append(": ").append(errorClass);
      }

      // Add error message if available
      if (errorMessage != null) {
        errorBuilder.append(": ").append(errorMessage);
      }

      return errorBuilder.toString();
    }
  }
}
