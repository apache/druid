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

package io.druid.query.extraction.namespace;

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
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class URIExtractionNamespaceTest
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
    ).registerSubtypes(URIExtractionNamespace.class, URIExtractionNamespace.FlatDataParser.class);

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
    URIExtractionNamespace.CSVFlatDataParser parser = new URIExtractionNamespace.CSVFlatDataParser(
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
    URIExtractionNamespace.CSVFlatDataParser parser = new URIExtractionNamespace.CSVFlatDataParser(
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
    URIExtractionNamespace.CSVFlatDataParser parser = new URIExtractionNamespace.CSVFlatDataParser(
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
    URIExtractionNamespace.TSVFlatDataParser parser = new URIExtractionNamespace.TSVFlatDataParser(
        ImmutableList.of("col1", "col2", "col3"),
        "|",
        "col2",
        "col3"
    );
    Assert.assertEquals(ImmutableMap.of("B", "C"), parser.getParser().parse("A|B|C"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadTSV()
  {
    URIExtractionNamespace.TSVFlatDataParser parser = new URIExtractionNamespace.TSVFlatDataParser(
        ImmutableList.of("col1", "col2", "col3fdsfds"),
        ",",
        "col2",
        "col3"
    );
    Map<String, String> map = parser.getParser().parse("A,B,C");
    Assert.assertEquals(ImmutableMap.of("B", "C"), parser.getParser().parse("A,B,C"));
  }


  @Test(expected = NullPointerException.class)
  public void testBadTSV2()
  {
    URIExtractionNamespace.TSVFlatDataParser parser = new URIExtractionNamespace.TSVFlatDataParser(
        ImmutableList.of("col1", "col2", "col3"),
        ",",
        "col2",
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
    URIExtractionNamespace.JSONFlatDataParser parser = new URIExtractionNamespace.JSONFlatDataParser(
        new ObjectMapper(),
        keyField,
        valueField
    );
    Assert.assertEquals(
        ImmutableMap.of("B", "C"),
        parser.getParser()
              .parse(
                  String.format(
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
    URIExtractionNamespace.JSONFlatDataParser parser = new URIExtractionNamespace.JSONFlatDataParser(
        new ObjectMapper(),
        keyField,
        valueField
    );
    Assert.assertEquals(
        ImmutableMap.of("B", "C"),
        parser.getParser()
              .parse(
                  String.format(
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
    URIExtractionNamespace.JSONFlatDataParser parser = new URIExtractionNamespace.JSONFlatDataParser(
        registerTypes(new ObjectMapper()),
        null,
        valueField
    );
    Assert.assertEquals(
        ImmutableMap.of("B", "C"),
        parser.getParser()
              .parse(
                  String.format(
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
    URIExtractionNamespace.JSONFlatDataParser parser = new URIExtractionNamespace.JSONFlatDataParser(
        registerTypes(new ObjectMapper()),
        keyField,
        null
    );
    Assert.assertEquals(
        ImmutableMap.of("B", "C"),
        parser.getParser()
              .parse(
                  String.format(
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
    URIExtractionNamespace.JSONFlatDataParser parser = new URIExtractionNamespace.JSONFlatDataParser(
        registerTypes(new ObjectMapper()),
        "",
        ""
    );
    Assert.assertEquals(
        ImmutableMap.of("B", "C"),
        parser.getParser()
              .parse(
                  String.format(
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
    URIExtractionNamespace.ObjectMapperFlatDataParser parser = new URIExtractionNamespace.ObjectMapperFlatDataParser(
        registerTypes(new ObjectMapper())
    );
    Assert.assertEquals(ImmutableMap.of("B", "C"), parser.getParser().parse("{\"B\":\"C\"}"));
  }

  @Test
  public void testSimpleJSONSerDe() throws IOException
  {
    final ObjectMapper mapper = registerTypes(new DefaultObjectMapper());
    for (URIExtractionNamespace.FlatDataParser parser : ImmutableList.of(
        new URIExtractionNamespace.CSVFlatDataParser(
            ImmutableList.of(
                "col1",
                "col2",
                "col3"
            ), "col2", "col3"
        ),
        new URIExtractionNamespace.ObjectMapperFlatDataParser(mapper),
        new URIExtractionNamespace.JSONFlatDataParser(mapper, "keyField", "valueField"),
        new URIExtractionNamespace.TSVFlatDataParser(ImmutableList.of("A", "B"), ",", "A", "B")
    )) {
      final String str = mapper.writeValueAsString(parser);
      final URIExtractionNamespace.FlatDataParser parser2 = mapper.readValue(
          str,
          URIExtractionNamespace.FlatDataParser.class
      );
      Assert.assertEquals(str, mapper.writeValueAsString(parser2));
    }
  }

  @Test
  public void testSimpleToString() throws IOException
  {
    final ObjectMapper mapper = registerTypes(new DefaultObjectMapper());
    for (URIExtractionNamespace.FlatDataParser parser : ImmutableList.of(
        new URIExtractionNamespace.CSVFlatDataParser(
            ImmutableList.of(
                "col1",
                "col2",
                "col3"
            ), "col2", "col3"
        ),
        new URIExtractionNamespace.ObjectMapperFlatDataParser(mapper),
        new URIExtractionNamespace.JSONFlatDataParser(mapper, "keyField", "valueField"),
        new URIExtractionNamespace.TSVFlatDataParser(ImmutableList.of("A", "B"), ",", "A", "B")
    )) {
      Assert.assertFalse(parser.toString().contains("@"));
    }
  }

  @Test
  public void testExplicitJson() throws IOException
  {
    final ObjectMapper mapper = registerTypes(new DefaultObjectMapper());
    URIExtractionNamespace namespace = mapper.readValue(
        "{\"type\":\"uri\", \"uri\":\"file:/foo\", \"namespaceParseSpec\":{\"format\":\"simpleJson\"}, \"pollPeriod\":\"PT5M\", \"versionRegex\":\"a.b.c\", \"namespace\":\"testNamespace\"}",
        URIExtractionNamespace.class
    );

    Assert.assertEquals(
        URIExtractionNamespace.ObjectMapperFlatDataParser.class.getCanonicalName(),
        namespace.getNamespaceParseSpec().getClass().getCanonicalName()
    );
    Assert.assertEquals("file:/foo", namespace.getUri().toString());
    Assert.assertEquals("testNamespace", namespace.getNamespace());
    Assert.assertEquals("a.b.c", namespace.getVersionRegex());
    Assert.assertEquals(5L * 60_000L, namespace.getPollMs());
  }

  @Test
  public void testFlatDataNumeric()
  {
    final String keyField = "keyField";
    final String valueField = "valueField";
    final int n = 341879;
    final String nString = String.format("%d", n);
    URIExtractionNamespace.JSONFlatDataParser parser = new URIExtractionNamespace.JSONFlatDataParser(
        new ObjectMapper(),
        keyField,
        valueField
    );
    Assert.assertEquals(
        "num string value",
        ImmutableMap.of("B", nString),
        parser.getParser()
              .parse(
                  String.format(
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
                  String.format(
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
                  String.format(
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
                  String.format(
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
    final URIExtractionNamespace.ObjectMapperFlatDataParser parser = new URIExtractionNamespace.ObjectMapperFlatDataParser(
        registerTypes(new DefaultObjectMapper())
    );
    final int n = 341879;
    final String nString = String.format("%d", n);
    Assert.assertEquals(
        ImmutableMap.of("key", nString),
        parser.getParser().parse(String.format("{\"key\":%d}", n))
    );
  }
}
