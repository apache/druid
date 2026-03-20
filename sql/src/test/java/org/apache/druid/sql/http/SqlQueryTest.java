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

package org.apache.druid.sql.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.server.initialization.jetty.HttpException;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class SqlQueryTest
{
  private final ObjectMapper objectMapper = new DefaultObjectMapper();

  @Test
  public void testFromHttpServletRequestWithJsonContentType() throws Exception
  {
    String jsonQuery = "{\"query\":\"SELECT 1\"}";
    HttpServletRequest request = createMockRequest("application/json", jsonQuery);

    SqlQuery result = SqlQuery.from(request, objectMapper);

    Assert.assertEquals("SELECT 1", result.getQuery());
  }

  @Test
  public void testFromHttpServletRequestWithJsonContentTypeAndCharset() throws Exception
  {
    String jsonQuery = "{\"query\":\"SELECT 2\"}";
    HttpServletRequest request = createMockRequest("application/json; charset=UTF-8", jsonQuery);

    SqlQuery result = SqlQuery.from(request, objectMapper);

    Assert.assertEquals("SELECT 2", result.getQuery());
  }

  @Test
  public void testFromHttpServletRequestWithJsonContentTypeAndMultipleParams() throws Exception
  {
    String jsonQuery = "{\"query\":\"SELECT 3\"}";
    HttpServletRequest request = createMockRequest("application/json; charset=UTF-8; boundary=something", jsonQuery);

    SqlQuery result = SqlQuery.from(request, objectMapper);

    Assert.assertEquals("SELECT 3", result.getQuery());
  }

  @Test
  public void testFromHttpServletRequestWithTextPlainContentType() throws Exception
  {
    String textQuery = "SELECT 4";
    HttpServletRequest request = createMockRequest("text/plain", textQuery);

    SqlQuery result = SqlQuery.from(request, objectMapper);

    Assert.assertEquals("SELECT 4", result.getQuery());
  }

  @Test
  public void testFromHttpServletRequestWithTextPlainContentTypeAndCharset() throws Exception
  {
    String textQuery = "SELECT 5";
    HttpServletRequest request = createMockRequest("text/plain; charset=ISO-8859-1", textQuery);

    SqlQuery result = SqlQuery.from(request, objectMapper);

    Assert.assertEquals("SELECT 5", result.getQuery());
  }

  @Test
  public void testFromHttpServletRequestWithFormUrlencodedContentType() throws Exception
  {
    String formQuery = "SELECT 6";
    HttpServletRequest request = createMockRequest("application/x-www-form-urlencoded", formQuery);

    SqlQuery result = SqlQuery.from(request, objectMapper);

    Assert.assertEquals("SELECT 6", result.getQuery());
  }

  @Test
  public void testFromHttpServletRequestWithFormUrlencodedContentTypeAndCharset() throws Exception
  {
    String formQuery = "SELECT 7";
    HttpServletRequest request = createMockRequest("application/x-www-form-urlencoded; charset=UTF-8", formQuery);

    SqlQuery result = SqlQuery.from(request, objectMapper);

    Assert.assertEquals("SELECT 7", result.getQuery());
  }

  @Test
  public void testFromHttpServletRequestWithNullContentType() throws Exception
  {
    String content = "SELECT 8";
    HttpServletRequest request = createMockRequest(null, content);

    HttpException exception = Assert.assertThrows(
        HttpException.class,
        () -> SqlQuery.from(request, objectMapper)
    );

    Assert.assertEquals(Response.Status.BAD_REQUEST, exception.getStatusCode());
    Assert.assertEquals("Missing Content-Type header", exception.getMessage());
  }

  @Test
  public void testFromHttpServletRequestWithUnsupportedContentType() throws Exception
  {
    String xmlQuery = "<query>SELECT 9</query>";
    HttpServletRequest request = createMockRequest("application/xml", xmlQuery);

    HttpException exception = Assert.assertThrows(
        HttpException.class,
        () -> SqlQuery.from(request, objectMapper)
    );

    Assert.assertEquals(Response.Status.UNSUPPORTED_MEDIA_TYPE, exception.getStatusCode());
    Assert.assertTrue(exception.getMessage().contains("Unsupported Content-Type"));
    Assert.assertTrue(exception.getMessage().contains("application/xml"));
  }

  @Test
  public void testFromHttpServletRequestWithUnsupportedContentTypeWithParams() throws Exception
  {
    String xmlQuery = "<query>SELECT 10</query>";
    HttpServletRequest request = createMockRequest("application/xml; charset=UTF-8", xmlQuery);

    HttpException exception = Assert.assertThrows(
        HttpException.class,
        () -> SqlQuery.from(request, objectMapper)
    );

    Assert.assertEquals(Response.Status.UNSUPPORTED_MEDIA_TYPE, exception.getStatusCode());
    Assert.assertTrue(exception.getMessage().contains("Unsupported Content-Type"));
    Assert.assertTrue(exception.getMessage().contains("application/xml"));
  }

  @Test
  public void testFromHttpServletRequestWithInvalidContentTypeFormat() throws Exception
  {
    String content = "SELECT 11";
    HttpServletRequest request = createMockRequest("invalid-content-type-format", content);

    HttpException exception = Assert.assertThrows(
        HttpException.class,
        () -> SqlQuery.from(request, objectMapper)
    );

    Assert.assertEquals(Response.Status.BAD_REQUEST, exception.getStatusCode());
    Assert.assertTrue(exception.getMessage().contains("Invalid Content-Type header"));
  }

  @Test
  public void testFromHttpServletRequestWithEmptyJsonQuery() throws Exception
  {
    String jsonQuery = "{}";
    HttpServletRequest request = createMockRequest("application/json", jsonQuery);

    HttpException exception = Assert.assertThrows(
        HttpException.class,
        () -> SqlQuery.from(request, objectMapper)
    );

    Assert.assertEquals(Response.Status.BAD_REQUEST, exception.getStatusCode());
    Assert.assertTrue(exception.getMessage()
                               .contains("Unable to read query from request: Cannot construct instance of "));
  }

  @Test
  public void testFromHttpServletRequestWithNullJsonQuery() throws Exception
  {
    String jsonQuery = "{\"query\": null}";
    HttpServletRequest request = createMockRequest("application/json", jsonQuery);

    HttpException exception = Assert.assertThrows(
        HttpException.class,
        () -> SqlQuery.from(request, objectMapper)
    );

    Assert.assertEquals(Response.Status.BAD_REQUEST, exception.getStatusCode());
  }

  @Test
  public void testFromHttpServletRequestWithEmptyTextQuery() throws Exception
  {
    String textQuery = "";
    HttpServletRequest request = createMockRequest("text/plain", textQuery);

    HttpException exception = Assert.assertThrows(
        HttpException.class,
        () -> SqlQuery.from(request, objectMapper)
    );

    Assert.assertEquals(Response.Status.BAD_REQUEST, exception.getStatusCode());
    Assert.assertEquals("Empty query", exception.getMessage());
  }

  @Test
  public void testFromHttpServletRequestWithWhitespaceOnlyTextQuery() throws Exception
  {
    String textQuery = "   \n\t  ";
    HttpServletRequest request = createMockRequest("text/plain", textQuery);

    HttpException exception = Assert.assertThrows(
        HttpException.class,
        () -> SqlQuery.from(request, objectMapper)
    );

    Assert.assertEquals(Response.Status.BAD_REQUEST, exception.getStatusCode());
    Assert.assertEquals("Empty query", exception.getMessage());
  }

  @Test
  public void testFromHttpServletRequestWithEmptyFormUrlencodedQuery() throws Exception
  {
    String formQuery = "";
    HttpServletRequest request = createMockRequest("application/x-www-form-urlencoded", formQuery);

    HttpException exception = Assert.assertThrows(
        HttpException.class,
        () -> SqlQuery.from(request, objectMapper)
    );

    Assert.assertEquals(Response.Status.BAD_REQUEST, exception.getStatusCode());
    Assert.assertEquals("Empty query", exception.getMessage());
  }


  @Test
  public void testFromHttpServletRequestWithMalformedJson() throws Exception
  {
    String malformedJson = "{\"query\":\"SELECT 12\""; // Missing closing brace
    HttpServletRequest request = createMockRequest("application/json", malformedJson);

    HttpException exception = Assert.assertThrows(
        HttpException.class,
        () -> SqlQuery.from(request, objectMapper)
    );

    Assert.assertEquals(Response.Status.BAD_REQUEST, exception.getStatusCode());
    Assert.assertTrue(exception.getMessage().contains("Malformed SQL query wrapped in JSON: Unexpected end-of-input:"));
  }

  @Test
  public void testFromHttpServletRequestWithCaseInsensitiveContentType() throws Exception
  {
    // Test various case combinations
    String jsonQuery = "{\"query\":\"SELECT 13\"}";

    HttpServletRequest request1 = createMockRequest("APPLICATION/JSON", jsonQuery);
    SqlQuery result1 = SqlQuery.from(request1, objectMapper);
    Assert.assertEquals("SELECT 13", result1.getQuery());

    HttpServletRequest request2 = createMockRequest("Application/Json; Charset=UTF-8", jsonQuery);
    SqlQuery result2 = SqlQuery.from(request2, objectMapper);
    Assert.assertEquals("SELECT 13", result2.getQuery());

    String textQuery = "SELECT 14";
    HttpServletRequest request3 = createMockRequest("TEXT/PLAIN", textQuery);
    SqlQuery result3 = SqlQuery.from(request3, objectMapper);
    Assert.assertEquals("SELECT 14", result3.getQuery());
  }

  @Test
  public void testFromHttpServletRequestWithComplexJsonQuery() throws Exception
  {
    String complexJsonQuery = "{\"query\":\"SELECT COUNT(*) FROM table WHERE col > 100\", \"context\":{\"timeout\":30000}, \"header\":true}";
    HttpServletRequest request = createMockRequest("application/json; charset=UTF-8", complexJsonQuery);

    SqlQuery result = SqlQuery.from(request, objectMapper);

    Assert.assertEquals("SELECT COUNT(*) FROM table WHERE col > 100", result.getQuery());
    Assert.assertTrue(result.includeHeader());
    Assert.assertNotNull(result.getContext());
  }


  private HttpServletRequest createMockRequest(String contentType, String content) throws IOException
  {
    HttpServletRequest request = EasyMock.createMock(HttpServletRequest.class);
    EasyMock.expect(request.getContentType()).andReturn(contentType).anyTimes();

    InputStream inputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    EasyMock.expect(request.getInputStream()).andReturn(new ServletInputStreamWrapper(inputStream)).anyTimes();

    EasyMock.replay(request);
    return request;
  }

  /**
   * Helper class to wrap InputStream as ServletInputStream since EasyMock needs concrete implementation
   */
  private static class ServletInputStreamWrapper extends javax.servlet.ServletInputStream
  {
    private final InputStream inputStream;

    public ServletInputStreamWrapper(InputStream inputStream)
    {
      this.inputStream = inputStream;
    }

    @Override
    public int read() throws IOException
    {
      return inputStream.read();
    }

    @Override
    public boolean isFinished()
    {
      try {
        return inputStream.available() == 0;
      }
      catch (IOException e) {
        return true;
      }
    }

    @Override
    public boolean isReady()
    {
      return true;
    }

    @Override
    public void setReadListener(javax.servlet.ReadListener readListener)
    {
      // Not implemented for testing
    }
  }
}
