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

package org.apache.druid.query.lookup.namespace;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Module;
import org.apache.druid.guice.GuiceAnnotationIntrospector;
import org.apache.druid.guice.GuiceInjectableValues;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.lookup.namespace.parsers.CSVFlatDataParser;
import org.apache.druid.query.lookup.namespace.parsers.FlatDataParser;
import org.apache.druid.query.lookup.namespace.parsers.JSONFlatDataParser;
import org.apache.druid.query.lookup.namespace.parsers.ObjectMapperFlatDataParser;
import org.apache.druid.query.lookup.namespace.parsers.TSVFlatDataParser;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class UriExtractionNamespaceTest
{
  public static ObjectMapper registerTypes(
      final ObjectMapper mapper
  )
  {
    mapper.setInjectableValues(
        new GuiceInjectableValues(
            Guice.createInjector(
                ImmutableList.of(
                    new Module()
                    {
                      @Override
                      public void configure(Binder binder)
                      {
                        binder.bind(ObjectMapper.class).annotatedWith(Json.class).toInstance(mapper);
                        binder.bind(ObjectMapper.class).toInstance(mapper);
                      }
                    }
                )
            )
        )
    ).registerSubtypes(UriExtractionNamespace.class, FlatDataParser.class);

    final GuiceAnnotationIntrospector guiceIntrospector = new GuiceAnnotationIntrospector();
    mapper.setAnnotationIntrospectors(
        new AnnotationIntrospectorPair(
            guiceIntrospector,
            mapper.getSerializationConfig().getAnnotationIntrospector()
        ),
        new AnnotationIntrospectorPair(
            guiceIntrospector,
            mapper.getDeserializationConfig().getAnnotationIntrospector()
        )
    );
    return mapper;
  }

  @Test
  public void testCSV()
  {
    CSVFlatDataParser parser = new CSVFlatDataParser(
        ImmutableList.of(
            "col1",
            "col2",
            "col3"
        ), "col2", "col3"
    );
    Assert.assertEquals(ImmutableMap.of("B", "C"), parser.getParser().parseToMap("A,B,C"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadCSV()
  {
    CSVFlatDataParser parser = new CSVFlatDataParser(
        ImmutableList.of(
            "col1",
            "col2",
            "col3"
        ), "col2", "col3ADFSDF"
    );
    Assert.assertEquals(ImmutableMap.of("B", "C"), parser.getParser().parseToMap("A,B,C"));
  }

  @Test(expected = NullPointerException.class)
  public void testBadCSV2()
  {
    CSVFlatDataParser parser = new CSVFlatDataParser(
        ImmutableList.of(
            "col1",
            "col2",
            "col3"
        ), "col2", "col3"
    );
    Map<String, String> map = parser.getParser().parseToMap("A");
  }

  @Test
  public void testTSV()
  {
    TSVFlatDataParser parser = new TSVFlatDataParser(
        ImmutableList.of("col1", "col2", "col3"),
        "|",
        null, "col2",
        "col3"
    );
    Assert.assertEquals(ImmutableMap.of("B", "C"), parser.getParser().parseToMap("A|B|C"));
  }

  @Test
  public void testWithListDelimiterTSV()
  {
    TSVFlatDataParser parser = new TSVFlatDataParser(
        ImmutableList.of("col1", "col2", "col3"),
        "\\u0001",
        "\\u0002", "col2",
        "col3"
    );
    Assert.assertEquals(ImmutableMap.of("B", "C"), parser.getParser().parseToMap("A\\u0001B\\u0001C"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadTSV()
  {
    TSVFlatDataParser parser = new TSVFlatDataParser(
        ImmutableList.of("col1", "col2", "col3fdsfds"),
        ",",
        null, "col2",
        "col3"
    );
    Map<String, String> map = parser.getParser().parseToMap("A,B,C");
    Assert.assertEquals(ImmutableMap.of("B", "C"), parser.getParser().parseToMap("A,B,C"));
  }


  @Test(expected = NullPointerException.class)
  public void testBadTSV2()
  {
    TSVFlatDataParser parser = new TSVFlatDataParser(
        ImmutableList.of("col1", "col2", "col3"),
        ",",
        null, "col2",
        "col3"
    );
    Map<String, String> map = parser.getParser().parseToMap("A");
    Assert.assertEquals(ImmutableMap.of("B", "C"), parser.getParser().parseToMap("A,B,C"));
  }

  @Test
  public void testJSONFlatDataParser()
  {
    final String keyField = "keyField";
    final String valueField = "valueField";
    JSONFlatDataParser parser = new JSONFlatDataParser(
        new ObjectMapper(),
        keyField,
        valueField
    );
    Assert.assertEquals(
        ImmutableMap.of("B", "C"),
        parser.getParser()
              .parseToMap(
                  StringUtils.format(
                      "{\"%s\":\"B\", \"%s\":\"C\", \"FOO\":\"BAR\"}",
                      keyField,
                      valueField
                  )
              )
    );
  }


  @Test(expected = NullPointerException.class)
  public void testJSONFlatDataParserBad()
  {
    final String keyField = "keyField";
    final String valueField = "valueField";
    JSONFlatDataParser parser = new JSONFlatDataParser(
        new ObjectMapper(),
        keyField,
        valueField
    );
    Assert.assertEquals(
        ImmutableMap.of("B", "C"),
        parser.getParser()
              .parseToMap(
                  StringUtils.format(
                      "{\"%sDFSDFDS\":\"B\", \"%s\":\"C\", \"FOO\":\"BAR\"}",
                      keyField,
                      valueField
                  )
              )
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testJSONFlatDataParserBad2()
  {
    final String keyField = "keyField";
    final String valueField = "valueField";
    JSONFlatDataParser parser = new JSONFlatDataParser(
        registerTypes(new ObjectMapper()),
        null,
        valueField
    );
    Assert.assertEquals(
        ImmutableMap.of("B", "C"),
        parser.getParser()
              .parseToMap(
                  StringUtils.format(
                      "{\"%sDFSDFDS\":\"B\", \"%s\":\"C\", \"FOO\":\"BAR\"}",
                      keyField,
                      valueField
                  )
              )
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testJSONFlatDataParserBad3()
  {
    final String keyField = "keyField";
    final String valueField = "valueField";
    JSONFlatDataParser parser = new JSONFlatDataParser(
        registerTypes(new ObjectMapper()),
        keyField,
        null
    );
    Assert.assertEquals(
        ImmutableMap.of("B", "C"),
        parser.getParser()
              .parseToMap(
                  StringUtils.format(
                      "{\"%sDFSDFDS\":\"B\", \"%s\":\"C\", \"FOO\":\"BAR\"}",
                      keyField,
                      valueField
                  )
              )
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testJSONFlatDataParserBad4()
  {
    final String keyField = "keyField";
    final String valueField = "valueField";
    JSONFlatDataParser parser = new JSONFlatDataParser(
        registerTypes(new ObjectMapper()),
        "",
        ""
    );
    Assert.assertEquals(
        ImmutableMap.of("B", "C"),
        parser.getParser()
              .parseToMap(
                  StringUtils.format(
                      "{\"%sDFSDFDS\":\"B\", \"%s\":\"C\", \"FOO\":\"BAR\"}",
                      keyField,
                      valueField
                  )
              )
    );
  }

  @Test
  public void testObjectMapperFlatDataParser()
  {
    ObjectMapperFlatDataParser parser = new ObjectMapperFlatDataParser(
        registerTypes(new ObjectMapper())
    );
    Assert.assertEquals(ImmutableMap.of("B", "C"), parser.getParser().parseToMap("{\"B\":\"C\"}"));
  }

  @Test
  public void testSimpleJSONSerDe() throws IOException
  {
    final ObjectMapper mapper = registerTypes(new DefaultObjectMapper());
    for (FlatDataParser parser : ImmutableList.of(
        new CSVFlatDataParser(
            ImmutableList.of(
                "col1",
                "col2",
                "col3"
            ), "col2", "col3"
        ),
        new ObjectMapperFlatDataParser(mapper),
        new JSONFlatDataParser(mapper, "keyField", "valueField"),
        new TSVFlatDataParser(ImmutableList.of("A", "B"), ",", null, "A", "B")
    )) {
      final String str = mapper.writeValueAsString(parser);
      final FlatDataParser parser2 = mapper.readValue(
          str,
          FlatDataParser.class
      );
      Assert.assertEquals(str, mapper.writeValueAsString(parser2));
    }
  }

  @Test
  public void testSimpleToString()
  {
    final ObjectMapper mapper = registerTypes(new DefaultObjectMapper());
    for (FlatDataParser parser : ImmutableList.of(
        new CSVFlatDataParser(
            ImmutableList.of(
                "col1",
                "col2",
                "col3"
            ), "col2", "col3"
        ),
        new ObjectMapperFlatDataParser(mapper),
        new JSONFlatDataParser(mapper, "keyField", "valueField"),
        new TSVFlatDataParser(ImmutableList.of("A", "B"), ",", null, "A", "B")
    )) {
      Assert.assertFalse(parser.toString().contains("@"));
    }
  }

  @Test
  public void testMatchedJson() throws IOException
  {
    final ObjectMapper mapper = registerTypes(new DefaultObjectMapper());
    UriExtractionNamespace namespace = mapper.readValue(
        "{\"type\":\"uri\", \"uriPrefix\":\"file:/foo\", \"namespaceParseSpec\":{\"format\":\"simpleJson\"}, \"pollPeriod\":\"PT5M\", \"versionRegex\":\"a.b.c\", \"namespace\":\"testNamespace\"}",
        UriExtractionNamespace.class
    );

    Assert.assertEquals(
        ObjectMapperFlatDataParser.class.getCanonicalName(),
        namespace.getNamespaceParseSpec().getClass().getCanonicalName()
    );
    Assert.assertEquals("file:/foo", namespace.getUriPrefix().toString());
    Assert.assertEquals("a.b.c", namespace.getFileRegex());
    Assert.assertEquals(5L * 60_000L, namespace.getPollMs());
  }

  @Test
  public void testExplicitJson() throws IOException
  {
    final ObjectMapper mapper = registerTypes(new DefaultObjectMapper());
    UriExtractionNamespace namespace = mapper.readValue(
        "{\"type\":\"uri\", \"uri\":\"file:/foo\", \"namespaceParseSpec\":{\"format\":\"simpleJson\"}, \"pollPeriod\":\"PT5M\"}",
        UriExtractionNamespace.class
    );

    Assert.assertEquals(
        ObjectMapperFlatDataParser.class.getCanonicalName(),
        namespace.getNamespaceParseSpec().getClass().getCanonicalName()
    );
    Assert.assertEquals("file:/foo", namespace.getUri().toString());
    Assert.assertEquals(5L * 60_000L, namespace.getPollMs());
  }

  @Test(expected = JsonMappingException.class)
  public void testExplicitJsonException() throws IOException
  {
    final ObjectMapper mapper = registerTypes(new DefaultObjectMapper());
    mapper.readValue(
        "{\"type\":\"uri\", \"uri\":\"file:/foo\", \"namespaceParseSpec\":{\"format\":\"simpleJson\"}, \"pollPeriod\":\"PT5M\", \"versionRegex\":\"a.b.c\", \"namespace\":\"testNamespace\"}",
        UriExtractionNamespace.class
    );
  }

  @Test
  public void testFlatDataNumeric()
  {
    final String keyField = "keyField";
    final String valueField = "valueField";
    final int n = 341879;
    final String nString = StringUtils.format("%d", n);
    JSONFlatDataParser parser = new JSONFlatDataParser(
        new ObjectMapper(),
        keyField,
        valueField
    );
    Assert.assertEquals(
        "num string value",
        ImmutableMap.of("B", nString),
        parser.getParser()
              .parseToMap(
                  StringUtils.format(
                      "{\"%s\":\"B\", \"%s\":\"%d\", \"FOO\":\"BAR\"}",
                      keyField,
                      valueField,
                      n
                  )
              )
    );
    Assert.assertEquals(
        "num string key",
        ImmutableMap.of(nString, "C"),
        parser.getParser()
              .parseToMap(
                  StringUtils.format(
                      "{\"%s\":\"%d\", \"%s\":\"C\", \"FOO\":\"BAR\"}",
                      keyField,
                      n,
                      valueField
                  )
              )
    );
    Assert.assertEquals(
        "num value",
        ImmutableMap.of("B", nString),
        parser.getParser()
              .parseToMap(
                  StringUtils.format(
                      "{\"%s\":\"B\", \"%s\":%d, \"FOO\":\"BAR\"}",
                      keyField,
                      valueField,
                      n
                  )
              )
    );
    Assert.assertEquals(
        "num key",
        ImmutableMap.of(nString, "C"),
        parser.getParser()
              .parseToMap(
                  StringUtils.format(
                      "{\"%s\":%d, \"%s\":\"C\", \"FOO\":\"BAR\"}",
                      keyField,
                      n,
                      valueField
                  )
              )
    );
  }

  @Test
  public void testSimpleJsonNumeric()
  {
    final ObjectMapperFlatDataParser parser = new ObjectMapperFlatDataParser(
        registerTypes(new DefaultObjectMapper())
    );
    final int n = 341879;
    final String nString = StringUtils.format("%d", n);
    Assert.assertEquals(
        ImmutableMap.of("key", nString),
        parser.getParser().parseToMap(StringUtils.format("{\"key\":%d}", n))
    );
  }
}
