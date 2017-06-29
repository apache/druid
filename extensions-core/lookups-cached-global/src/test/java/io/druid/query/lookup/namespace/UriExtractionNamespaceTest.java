/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.lookup.namespace;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Module;
import io.druid.guice.GuiceAnnotationIntrospector;
import io.druid.guice.GuiceInjectableValues;
import io.druid.guice.annotations.Json;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.StringUtils;
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
    ).registerSubtypes(UriExtractionNamespace.class, UriExtractionNamespace.FlatDataParser.class);

    final GuiceAnnotationIntrospector guiceIntrospector = new GuiceAnnotationIntrospector();
    mapper.setAnnotationIntrospectors(
        new AnnotationIntrospectorPair(
            guiceIntrospector, mapper.getSerializationConfig().getAnnotationIntrospector()
        ),
        new AnnotationIntrospectorPair(
            guiceIntrospector, mapper.getDeserializationConfig().getAnnotationIntrospector()
        )
    );
    return mapper;
  }

  @Test
  public void testCSV()
  {
    UriExtractionNamespace.CSVFlatDataParser parser = new UriExtractionNamespace.CSVFlatDataParser(
        ImmutableList.of(
            "col1",
            "col2",
            "col3"
        ), "col2", "col3"
    );
    Assert.assertEquals(ImmutableMap.of("B", "C"), parser.getParser().parse("A,B,C"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadCSV()
  {
    UriExtractionNamespace.CSVFlatDataParser parser = new UriExtractionNamespace.CSVFlatDataParser(
        ImmutableList.of(
            "col1",
            "col2",
            "col3"
        ), "col2", "col3ADFSDF"
    );
    Assert.assertEquals(ImmutableMap.of("B", "C"), parser.getParser().parse("A,B,C"));
  }

  @Test(expected = NullPointerException.class)
  public void testBadCSV2()
  {
    UriExtractionNamespace.CSVFlatDataParser parser = new UriExtractionNamespace.CSVFlatDataParser(
        ImmutableList.of(
            "col1",
            "col2",
            "col3"
        ), "col2", "col3"
    );
    Map<String, String> map = parser.getParser().parse("A");
  }

  @Test
  public void testTSV()
  {
    UriExtractionNamespace.TSVFlatDataParser parser = new UriExtractionNamespace.TSVFlatDataParser(
        ImmutableList.of("col1", "col2", "col3"),
        "|",
        null, "col2",
        "col3"
    );
    Assert.assertEquals(ImmutableMap.of("B", "C"), parser.getParser().parse("A|B|C"));
  }

  @Test
  public void testWithListDelimiterTSV()
  {
    UriExtractionNamespace.TSVFlatDataParser parser = new UriExtractionNamespace.TSVFlatDataParser(
        ImmutableList.of("col1", "col2", "col3"),
        "\\u0001",
        "\\u0002", "col2",
        "col3"
    );
    Assert.assertEquals(ImmutableMap.of("B", "C"), parser.getParser().parse("A\\u0001B\\u0001C"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadTSV()
  {
    UriExtractionNamespace.TSVFlatDataParser parser = new UriExtractionNamespace.TSVFlatDataParser(
        ImmutableList.of("col1", "col2", "col3fdsfds"),
        ",",
        null, "col2",
        "col3"
    );
    Map<String, String> map = parser.getParser().parse("A,B,C");
    Assert.assertEquals(ImmutableMap.of("B", "C"), parser.getParser().parse("A,B,C"));
  }


  @Test(expected = NullPointerException.class)
  public void testBadTSV2()
  {
    UriExtractionNamespace.TSVFlatDataParser parser = new UriExtractionNamespace.TSVFlatDataParser(
        ImmutableList.of("col1", "col2", "col3"),
        ",",
        null, "col2",
        "col3"
    );
    Map<String, String> map = parser.getParser().parse("A");
    Assert.assertEquals(ImmutableMap.of("B", "C"), parser.getParser().parse("A,B,C"));
  }

  @Test
  public void testJSONFlatDataParser()
  {
    final String keyField = "keyField";
    final String valueField = "valueField";
    UriExtractionNamespace.JSONFlatDataParser parser = new UriExtractionNamespace.JSONFlatDataParser(
        new ObjectMapper(),
        keyField,
        valueField
    );
    Assert.assertEquals(
        ImmutableMap.of("B", "C"),
        parser.getParser()
              .parse(
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
    UriExtractionNamespace.JSONFlatDataParser parser = new UriExtractionNamespace.JSONFlatDataParser(
        new ObjectMapper(),
        keyField,
        valueField
    );
    Assert.assertEquals(
        ImmutableMap.of("B", "C"),
        parser.getParser()
              .parse(
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
    UriExtractionNamespace.JSONFlatDataParser parser = new UriExtractionNamespace.JSONFlatDataParser(
        registerTypes(new ObjectMapper()),
        null,
        valueField
    );
    Assert.assertEquals(
        ImmutableMap.of("B", "C"),
        parser.getParser()
              .parse(
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
    UriExtractionNamespace.JSONFlatDataParser parser = new UriExtractionNamespace.JSONFlatDataParser(
        registerTypes(new ObjectMapper()),
        keyField,
        null
    );
    Assert.assertEquals(
        ImmutableMap.of("B", "C"),
        parser.getParser()
              .parse(
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
    UriExtractionNamespace.JSONFlatDataParser parser = new UriExtractionNamespace.JSONFlatDataParser(
        registerTypes(new ObjectMapper()),
        "",
        ""
    );
    Assert.assertEquals(
        ImmutableMap.of("B", "C"),
        parser.getParser()
              .parse(
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
    UriExtractionNamespace.ObjectMapperFlatDataParser parser = new UriExtractionNamespace.ObjectMapperFlatDataParser(
        registerTypes(new ObjectMapper())
    );
    Assert.assertEquals(ImmutableMap.of("B", "C"), parser.getParser().parse("{\"B\":\"C\"}"));
  }

  @Test
  public void testSimpleJSONSerDe() throws IOException
  {
    final ObjectMapper mapper = registerTypes(new DefaultObjectMapper());
    for (UriExtractionNamespace.FlatDataParser parser : ImmutableList.of(
        new UriExtractionNamespace.CSVFlatDataParser(
            ImmutableList.of(
                "col1",
                "col2",
                "col3"
            ), "col2", "col3"
        ),
        new UriExtractionNamespace.ObjectMapperFlatDataParser(mapper),
        new UriExtractionNamespace.JSONFlatDataParser(mapper, "keyField", "valueField"),
        new UriExtractionNamespace.TSVFlatDataParser(ImmutableList.of("A", "B"), ",", null, "A", "B")
    )) {
      final String str = mapper.writeValueAsString(parser);
      final UriExtractionNamespace.FlatDataParser parser2 = mapper.readValue(
          str,
          UriExtractionNamespace.FlatDataParser.class
      );
      Assert.assertEquals(str, mapper.writeValueAsString(parser2));
    }
  }

  @Test
  public void testSimpleToString() throws IOException
  {
    final ObjectMapper mapper = registerTypes(new DefaultObjectMapper());
    for (UriExtractionNamespace.FlatDataParser parser : ImmutableList.of(
        new UriExtractionNamespace.CSVFlatDataParser(
            ImmutableList.of(
                "col1",
                "col2",
                "col3"
            ), "col2", "col3"
        ),
        new UriExtractionNamespace.ObjectMapperFlatDataParser(mapper),
        new UriExtractionNamespace.JSONFlatDataParser(mapper, "keyField", "valueField"),
        new UriExtractionNamespace.TSVFlatDataParser(ImmutableList.of("A", "B"), ",", null, "A", "B")
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
        UriExtractionNamespace.ObjectMapperFlatDataParser.class.getCanonicalName(),
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
        UriExtractionNamespace.ObjectMapperFlatDataParser.class.getCanonicalName(),
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
    UriExtractionNamespace.JSONFlatDataParser parser = new UriExtractionNamespace.JSONFlatDataParser(
        new ObjectMapper(),
        keyField,
        valueField
    );
    Assert.assertEquals(
        "num string value",
        ImmutableMap.of("B", nString),
        parser.getParser()
              .parse(
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
              .parse(
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
              .parse(
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
              .parse(
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
    final UriExtractionNamespace.ObjectMapperFlatDataParser parser = new UriExtractionNamespace.ObjectMapperFlatDataParser(
        registerTypes(new DefaultObjectMapper())
    );
    final int n = 341879;
    final String nString = StringUtils.format("%d", n);
    Assert.assertEquals(
        ImmutableMap.of("key", nString),
        parser.getParser().parse(StringUtils.format("{\"key\":%d}", n))
    );
  }
}
