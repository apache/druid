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

package io.druid.query.lookup.namespace;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.CharSink;
import com.google.common.io.Files;
import io.druid.data.input.MapPopulator;
import io.druid.jackson.DefaultObjectMapper;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class JSONFlatDataParserTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private static final String KEY = "foo";
  private static final String VAL = "bar";
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  private File tmpFile;

  @Before
  public void setUp() throws Exception
  {
    tmpFile = temporaryFolder.newFile("lookup.json");
    final CharSink sink = Files.asByteSink(tmpFile).asCharSink(Charsets.UTF_8);
    sink.write("{\"key\":\"" + KEY + "\",\"val\":\"" + VAL + "\"}");
  }

  @Test
  public void testSimpleParse() throws Exception
  {
    final URIExtractionNamespace.JSONFlatDataParser parser = new URIExtractionNamespace.JSONFlatDataParser(
        MAPPER,
        "key",
        "val"
    );
    final Map<String, String> map = new HashMap<>();
    new MapPopulator<>(parser.getParser()).populate(Files.asByteSource(tmpFile), map);
    Assert.assertEquals(VAL, map.get(KEY));
  }

  @Test
  public void testFailParse() throws Exception
  {
    expectedException.expect(new BaseMatcher<Object>()
    {
      @Override
      public boolean matches(Object o)
      {
        if (!(o instanceof NullPointerException)) {
          return false;
        }
        final NullPointerException npe = (NullPointerException) o;
        return npe.getMessage().startsWith("Key column [keyWHOOPS] missing data in line");
      }

      @Override
      public void describeTo(Description description)
      {

      }
    });
    final URIExtractionNamespace.JSONFlatDataParser parser = new URIExtractionNamespace.JSONFlatDataParser(
        MAPPER,
        "keyWHOOPS",
        "val"
    );
    final Map<String, String> map = new HashMap<>();
    new MapPopulator<>(parser.getParser()).populate(Files.asByteSource(tmpFile), map);
    Assert.assertEquals(VAL, map.get(KEY));
  }
}
