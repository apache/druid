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

package org.apache.druid.client;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.druid.client.JsonParserIterator.ResultStructure;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.context.ResponseContext;
import org.junit.Test;

import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test the syntax rules for the JSON parser iterator:
 * handling of array and object values, bogus values, etc.
 */
public class JsonParserIteratorSyntaxTest
{
  private static final String URL = "http:/host/resource";
  private static final String HOST = "host";
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();
  private static final JavaType JAVA_TYPE = OBJECT_MAPPER.getTypeFactory().constructType(String.class);

  private static class InputFuture implements Future<InputStream>
  {
    private final String json;

    private InputFuture(String json)
    {
      this.json = json;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning)
    {
      return false;
    }

    @Override
    public boolean isCancelled()
    {
      return false;
    }

    @Override
    public boolean isDone()
    {
      return false;
    }

    @Override
    public InputStream get()
    {
      if (json == null) {
        return null;
      }
      return new ReaderInputStream(new StringReader(json), StandardCharsets.UTF_8);
    }

    @Override
    public InputStream get(long timeout, TimeUnit unit)
    {
      return get();
    }
  }

  private JsonParserIterator<String> createIterator(
      ResultStructure kind,
      String json)
  {
    return new JsonParserIterator<>(
        kind,
        JAVA_TYPE,
        new InputFuture(json),
        URL,
        null,
        HOST,
        OBJECT_MAPPER
        );
  }

  @Test
  public void testNullInput()
  {
    for (ResultStructure kind : ResultStructure.values()) {
      try {
        createIterator(kind, null).hasNext();
        fail();
      }
      catch (ResourceLimitExceededException e) {
        // Expected
      }
    }
  }

  @Test
  public void testEmptyArrayInput()
  {
    for (ResultStructure kind : ResultStructure.values()) {
      try {
        createIterator(kind, "").hasNext();
        fail();
      }
      catch (QueryInterruptedException e) {
        assertTrue(e.getMessage().startsWith("Unexpected EOF"));
      }
    }
  }

  @Test
  public void testScalarInput()
  {
    try {
      createIterator(ResultStructure.ARRAY, "10").hasNext();
      fail();
    }
    catch (QueryInterruptedException e) {
      assertTrue(e.getMessage().startsWith("Expected START_ARRAY"));
    }
    catch (Exception e) {
      fail();
    }
    try {
      createIterator(ResultStructure.OBJECT, "10").hasNext();
      fail();
    }
    catch (QueryInterruptedException e) {
      assertTrue(e.getMessage().startsWith("Expected START_OBJECT"));
    }
    catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testMalformedObject()
  {
    // The JSON parser catches malformed JSON
    try {
      createIterator(ResultStructure.ARRAY, "{").hasNext();
      fail();
    }
    catch (QueryInterruptedException e) {
      assertTrue(e.getMessage().startsWith("Unexpected end-of-input"));
    }
  }

  /**
   * The JSON iterator expects at least one field.
   */
  @Test
  public void testEmptyObject()
  {
    final String input = "{}";
    for (ResultStructure kind : ResultStructure.values()) {
      try {
        createIterator(kind, input).hasNext();
        fail();
      }
      catch (QueryInterruptedException e) {
        assertTrue(e.getMessage().startsWith("Expected FIELD_NAME"));
      }
    }
  }

  @Test
  public void testUnkownObject()
  {
    final String input = "{\"foo\": 10}";
    for (ResultStructure kind : ResultStructure.values()) {
      try {
        createIterator(kind, input).hasNext();
        fail();
      }
      catch (QueryInterruptedException e) {
        assertTrue(e.getMessage().startsWith("Unexpected starting field[foo]"));
      }
    }
  }

  /**
   * The minimal acceptable error result is to include the "error" field
   * as that's how the parser knows its an error.
   */
  @Test
  public void testMinimalErrorResult()
  {
    final String input = "{\"error\": \"bad\"}";
    for (ResultStructure kind : ResultStructure.values()) {
      try {
        createIterator(kind, input).hasNext();
        fail();
      }
      catch (QueryInterruptedException e) {
        assertEquals("bad", e.getMessage());
      }
    }
  }

  /**
   * Other error fields can occur, but must occur after "error".
   * This is a harmless deviation from standard JSON which says
   * fields can occur in any order.
   */
  @Test
  public void testFullErrorResult()
  {
    final String input = "{\"error\": \"bad\", \"errorMessage\": \"msg\"}";
    for (ResultStructure kind : ResultStructure.values()) {
      try {
        createIterator(kind, input).hasNext();
        fail();
      }
      catch (QueryInterruptedException e) {
        assertEquals("msg", e.getMessage());
      }
    }
  }

  @Test
  public void testArrayResult()
  {
    final String input = "[\"first\", \"second\"]";
    for (ResultStructure kind : ResultStructure.values()) {
      JsonParserIterator<String> iter = createIterator(kind, input);
      assertTrue(iter.hasNext());
      assertEquals("first", iter.next());
      assertTrue(iter.hasNext());
      assertEquals("second", iter.next());
      assertFalse(iter.hasNext());
      assertEquals(2, iter.getResultRows());
      assertTrue(iter.isSuccess());
    }
  }

  @Test
  public void testEmptyArrayResult()
  {
    final String input = "[]";
    for (ResultStructure kind : ResultStructure.values()) {
      JsonParserIterator<String> iter = createIterator(kind, input);
      assertFalse(iter.hasNext());
      assertEquals(0, iter.getResultRows());
      assertTrue(iter.isSuccess());
    }
  }

  /**
   * Nulls should not occur in the result list. But, if they
   * do, ensure the parser handles them.
   */
  @Test
  public void testNullArrayResult()
  {
    final String input = "[\"first\", null]";
    for (ResultStructure kind : ResultStructure.values()) {
      Iterator<String> iter = createIterator(kind, input);
      assertTrue(iter.hasNext());
      assertEquals("first", iter.next());
      assertTrue(iter.hasNext());
      assertNull(iter.next());
      assertFalse(iter.hasNext());
    }
  }

  /**
   * Fail if the server returns an object format when we asked for
   * an array.
   */
  @Test
  public void testObjectForArray()
  {
    final String input = "{\"results\": []}";
    try {
      createIterator(ResultStructure.ARRAY, input).hasNext();
      fail();
    }
    catch (QueryInterruptedException e) {
      assertTrue(e.getMessage().startsWith("Got object format, expected array"));
    }
  }

  /**
   * The minimal object format is just an empty results array.
   */
  @Test
  public void testEmptyObjectResults()
  {
    final String input = "{\"results\": []}";
    Iterator<String> iter = createIterator(ResultStructure.OBJECT, input);
    assertFalse(iter.hasNext());
  }

  @Test
  public void testObjectResults()
  {
    final String input = "{\"results\": [\"first\", \"second\"]}";
    JsonParserIterator<String> iter = createIterator(ResultStructure.OBJECT, input);
    assertTrue(iter.hasNext());
    assertEquals("first", iter.next());
    assertTrue(iter.hasNext());
    assertEquals("second", iter.next());
    assertFalse(iter.hasNext());
    assertEquals(2, iter.getResultRows());
    assertTrue(iter.isSuccess());
  }

  /**
   * For forward-compatibility, the parser ignores unexpected
   * fields in the object result. Verify that the parser "free wheels"
   * over any unexpected values, even if structured.
   */
  @Test
  public void testUnexpectedObjectField()
  {
    {
      final String input = "{\"results\": [], \"bogus\": 10}";
      JsonParserIterator<String> iter = createIterator(ResultStructure.OBJECT, input);
      assertFalse(iter.hasNext());
      assertEquals(0, iter.getResultRows());
      assertTrue(iter.isSuccess());
      assertNull(iter.responseTrailer());
    }
    {
      final String input = "{\"results\": [], " +
          "\"bogus\": {\"foo\": 10}}";
      JsonParserIterator<String> iter = createIterator(ResultStructure.OBJECT, input);
      assertFalse(iter.hasNext());
      assertEquals(0, iter.getResultRows());
      assertTrue(iter.isSuccess());
      assertNull(iter.responseTrailer());
    }
    {
      final String input = "{\"results\": [], " +
          "\"bogus\": [10]}";
      JsonParserIterator<String> iter = createIterator(ResultStructure.OBJECT, input);
      assertFalse(iter.hasNext());
      assertEquals(0, iter.getResultRows());
      assertTrue(iter.isSuccess());
      assertNull(iter.responseTrailer());
    }
  }

  /**
   * We finally get to the point of all the above: the response
   * trailer, which itself may have unknown fields.
   */
  @Test
  public void testTrailer()
  {
    final String input = "{\"results\": [\"first\", \"second\"]," +
          "\"context\": {\"ETag\": \"the tag\"," +
          "\"bogus\": 10}}";
    JsonParserIterator<String> iter = createIterator(ResultStructure.OBJECT, input);
    assertTrue(iter.hasNext());
    assertEquals("first", iter.next());
    assertTrue(iter.hasNext());
    assertEquals("second", iter.next());
    assertFalse(iter.hasNext());
    assertEquals(2, iter.getResultRows());
    assertTrue(iter.isSuccess());
    ResponseContext context = iter.responseTrailer();
    assertNotNull(context);
    assertEquals("the tag", context.getEntityTag());
    assertEquals(1, context.toMap().size());
  }
}
