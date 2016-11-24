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
import io.druid.java.util.common.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
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
    mapper.registerSubtypes(URIExtractionNamespace.class, URIExtractionNamespace.CSVFlatDataParser.class);

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

  final String id = "test";
  final String mapName = "testMap";

  @Test
  public void testCSV()
  {
    URIExtractionNamespace.FlatDataParser parser = new URIExtractionNamespace.CSVFlatDataParser(
        ImmutableList.of(
            "col1",
            "col2",
            "col3"
        ),
        ImmutableList.of(new KeyValueMap(mapName, "col2", "col3"))
    ).withID(id);
    Assert.assertEquals(ImmutableMap.of("B", "C"), parser.parse("A,B,C").get(new Pair(id, mapName)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadCSV()
  {
    URIExtractionNamespace.FlatDataParser parser = new URIExtractionNamespace.CSVFlatDataParser(
        ImmutableList.of(
            "col1",
            "col2",
            "col3"
        ),
        ImmutableList.of(new KeyValueMap(mapName, "col2", "col3ADFSDF"))
    ).withID(id);
    Assert.assertEquals(ImmutableMap.of("B", "C"), parser.parse("A,B,C").get(new Pair(id, mapName)));
  }

  @Test(expected = NullPointerException.class)
  public void testBadCSV2()
  {
    URIExtractionNamespace.FlatDataParser parser = new URIExtractionNamespace.CSVFlatDataParser(
        ImmutableList.of(
            "col1",
            "col2",
            "col3"
        ),
        ImmutableList.of(new KeyValueMap(mapName, "col2", "col3"))
    ).withID(id);
    Assert.assertEquals(ImmutableMap.of("B", "C"), parser.parse("A").get(new Pair(id, mapName)));
  }

  @Test
  public void testTSV()
  {
    URIExtractionNamespace.FlatDataParser parser = new URIExtractionNamespace.TSVFlatDataParser(
        ImmutableList.of("col1", "col2", "col3"),
        "|",
        null,
        ImmutableList.of(new KeyValueMap(mapName, "col2", "col3"))
    ).withID(id);
    Assert.assertEquals(ImmutableMap.of("B", "C"), parser.parse("A|B|C").get(new Pair(id, mapName)));
  }

  @Test
  public void testWithListDelimiterTSV()
  {
    URIExtractionNamespace.FlatDataParser parser = new URIExtractionNamespace.TSVFlatDataParser(
        ImmutableList.of("col1", "col2", "col3"),
        "\\u0001",
        "\\u0002",
        ImmutableList.of(new KeyValueMap(mapName, "col2", "col3"))
    ).withID(id);
    Assert.assertEquals(ImmutableMap.of("B", "C"), parser.parse("A\\u0001B\\u0001C").get(new Pair(id, mapName)));
  }


  @Test(expected = IllegalArgumentException.class)
  public void testBadTSV()
  {
    URIExtractionNamespace.FlatDataParser parser = new URIExtractionNamespace.TSVFlatDataParser(
        ImmutableList.of("col1", "col2", "col3fdsfds"),
        ",",
        null,
        ImmutableList.of(new KeyValueMap(mapName, "col2", "col3"))
    ).withID(id);
    Map<String, String> map = parser.parse("A,B,C").get(new Pair(id, mapName));
    Assert.assertEquals(ImmutableMap.of("B", "C"), parser.parse("A,B,C").get(new Pair(id, mapName)));
  }

  @Test(expected = NullPointerException.class)
  public void testBadTSV2()
  {
    URIExtractionNamespace.FlatDataParser parser = new URIExtractionNamespace.TSVFlatDataParser(
        ImmutableList.of("col1", "col2", "col3"),
        ",",
        null,
        ImmutableList.of(new KeyValueMap(mapName, "col2", "col3"))
    ).withID(id);
    Map<String, String> map = parser.parse("A").get(new Pair(id, mapName));
    Assert.assertEquals(ImmutableMap.of("B", "C"), parser.parse("A,B,C").get(new Pair(id, mapName)));
  }

  @Test
  public void testJSONFlatDataParser()
  {
    final String keyField = "keyField";
    final String valueField = "valueField";
    URIExtractionNamespace.FlatDataParser parser = new URIExtractionNamespace.JSONFlatDataParser(
        new ObjectMapper(),
        ImmutableList.of(new KeyValueMap(mapName, keyField, valueField))
    ).withID(id);
    Assert.assertEquals(
        ImmutableMap.of("B", "C"),
        parser.parse(
            String.format(
                "{\"%s\":\"B\", \"%s\":\"C\", \"FOO\":\"BAR\"}",
                keyField,
                valueField
            )
        ).get(new Pair(id, mapName))
    );
  }

  @Test(expected = NullPointerException.class)
  public void testJSONFlatDataParserBad()
  {
    final String keyField = "keyField";
    final String valueField = "valueField";
    URIExtractionNamespace.FlatDataParser parser = new URIExtractionNamespace.JSONFlatDataParser(
        new ObjectMapper(),
        ImmutableList.of(new KeyValueMap(mapName, keyField, valueField))
    ).withID(id);
    Assert.assertEquals(
        ImmutableMap.of("B", "C"),
        parser.parse(
            String.format(
                "{\"%sDFSDFDS\":\"B\", \"%s\":\"C\", \"FOO\":\"BAR\"}",
                keyField,
                valueField
            )
        ).get(new Pair(id, mapName))
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testJSONFlatDataParserBad2()
  {
    final String keyField = "keyField";
    final String valueField = "valueField";
    URIExtractionNamespace.FlatDataParser parser = new URIExtractionNamespace.JSONFlatDataParser(
        new ObjectMapper(),
        ImmutableList.of(new KeyValueMap(mapName, null, valueField))
    ).withID(id);
    Assert.assertEquals(
        ImmutableMap.of("B", "C"),
        parser.parse(
            String.format(
                "{\"%sDFSDFDS\":\"B\", \"%s\":\"C\", \"FOO\":\"BAR\"}",
                keyField,
                valueField
            )
        ).get(new Pair(id, mapName))
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testJSONFlatDataParserBad3()
  {
    final String keyField = "keyField";
    final String valueField = "valueField";
    URIExtractionNamespace.FlatDataParser parser = new URIExtractionNamespace.JSONFlatDataParser(
        new ObjectMapper(),
        ImmutableList.of(new KeyValueMap(mapName, keyField, null))
    ).withID(id);
    Assert.assertEquals(
        ImmutableMap.of("B", "C"),
        parser.parse(
            String.format(
                "{\"%sDFSDFDS\":\"B\", \"%s\":\"C\", \"FOO\":\"BAR\"}",
                keyField,
                valueField
            )
        ).get(new Pair(id, mapName))
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testJSONFlatDataParserBad4()
  {
    final String keyField = "keyField";
    final String valueField = "valueField";
    URIExtractionNamespace.FlatDataParser parser = new URIExtractionNamespace.JSONFlatDataParser(
        new ObjectMapper(),
        ImmutableList.of(new KeyValueMap(mapName, "", ""))
    ).withID(id);
    Assert.assertEquals(
        ImmutableMap.of("B", "C"),
        parser.parse(
            String.format(
                "{\"%sDFSDFDS\":\"B\", \"%s\":\"C\", \"FOO\":\"BAR\"}",
                keyField,
                valueField
            )
        ).get(new Pair(id, mapName))
    );
  }

  @Test
  public void testObjectMapperFlatDataParser()
  {
    URIExtractionNamespace.FlatDataParser parser = new URIExtractionNamespace.ObjectMapperFlatDataParser(
        registerTypes(new ObjectMapper())
    ).withID(id);
    Assert.assertEquals(
        ImmutableMap.of("B", "C"),
        parser.parse("{\"B\":\"C\"}").get(new Pair(id, KeyValueMap.DEFAULT_MAPNAME))
    );
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
            ),
            ImmutableList.of(new KeyValueMap(mapName, "col2", "col3"))
        ).withID(id),
        new URIExtractionNamespace.ObjectMapperFlatDataParser(mapper).withID(id),
        new URIExtractionNamespace.JSONFlatDataParser(
            mapper,
            ImmutableList.of(new KeyValueMap(mapName, "col2", "col3"))
        ).withID(id),
        new URIExtractionNamespace.TSVFlatDataParser(
            ImmutableList.of(
                "col1",
                "col2",
                "col3"
            ),
            ",",
            null,
            ImmutableList.of(new KeyValueMap(mapName, "col2", "col3"))
        ).withID(id)
    )) {
      final String str = mapper.writeValueAsString(parser);
      final URIExtractionNamespace.FlatDataParser parser2 = mapper.readValue(
          str,
          URIExtractionNamespace.FlatDataParser.class
      ).withID(id);
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
            ),
            ImmutableList.of(new KeyValueMap(mapName, "col2", "col3"))
        ).withID(id),
        new URIExtractionNamespace.ObjectMapperFlatDataParser(mapper).withID(id),
        new URIExtractionNamespace.JSONFlatDataParser(
            mapper,
            ImmutableList.of(new KeyValueMap(mapName, "col2", "col3"))
        ).withID(id),
        new URIExtractionNamespace.TSVFlatDataParser(
            ImmutableList.of(
                "col1",
                "col2",
                "col3"
            ),
            ",",
            null,
            ImmutableList.of(new KeyValueMap(mapName, "col2", "col3"))
        ).withID(id)
    )) {
      Assert.assertFalse(parser.toString().contains("@"));
    }
  }

  @Test
  public void testMatchedJson() throws IOException
  {
    final ObjectMapper mapper = registerTypes(new DefaultObjectMapper());
    URIExtractionNamespace namespace = mapper.readValue(
        "{\"type\":\"uri\", \"uriPrefix\":\"file:/foo\", \"namespaceParseSpec\":{\"format\":\"simpleJson\"}, \"pollPeriod\":\"PT5M\", \"versionRegex\":\"a.b.c\"}",
        URIExtractionNamespace.class
    );

    Assert.assertEquals(
        URIExtractionNamespace.ObjectMapperFlatDataParser.class.getCanonicalName(),
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
    URIExtractionNamespace namespace = mapper.readValue(
        "{\"type\":\"uri\", \"uri\":\"file:/foo\", \"namespaceParseSpec\":{\"format\":\"simpleJson\"}, \"pollPeriod\":\"PT5M\"}",
        URIExtractionNamespace.class
    );

    Assert.assertEquals(
        URIExtractionNamespace.ObjectMapperFlatDataParser.class.getCanonicalName(),
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
        "{\"type\":\"uri\", \"uri\":\"file:/foo\", \"namespaceParseSpec\":{\"format\":\"simpleJson\"}, \"pollPeriod\":\"PT5M\", \"versionRegex\":\"a.b.c\"}",
        URIExtractionNamespace.class
    );
  }

  @Test
  public void testExplicitJsonWithMultipleMaps() throws IOException
  {
    final ObjectMapper mapper = registerTypes(new DefaultObjectMapper());
    URIExtractionNamespace namespace = mapper.readValue(
        "{\"type\":\"uri\", \"uri\":\"file:/foo\", \"namespaceParseSpec\":{\"format\":\"csv\", \"columns\":[\"key\", \"value1\", \"value2\"], \"maps\":[{\"mapName\":\"map1\",\"keyColumn\":\"key\", \"valueColumn\":\"value1\"}, {\"mapName\":\"map2\",\"keyColumn\":\"key\", \"valueColumn\":\"value2\"}]}, \"pollPeriod\":\"PT5M\"}",
        URIExtractionNamespace.class
    );

    Assert.assertEquals(
        URIExtractionNamespace.CSVFlatDataParser.class.getCanonicalName(),
        namespace.getNamespaceParseSpec().getClass().getCanonicalName()
    );
    Assert.assertEquals("file:/foo", namespace.getUri().toString());
    URIExtractionNamespace.CSVFlatDataParser parser = (URIExtractionNamespace.CSVFlatDataParser)namespace.getNamespaceParseSpec();
    List<KeyValueMap> keyValueMaps = parser.getMaps();
    Assert.assertEquals(2, keyValueMaps.size());
    Assert.assertEquals("map1", keyValueMaps.get(0).getMapName());
    Assert.assertEquals("key", keyValueMaps.get(0).getKeyColumn());
    Assert.assertEquals("value1", keyValueMaps.get(0).getValueColumn());
    Assert.assertEquals("map2", keyValueMaps.get(1).getMapName());
    Assert.assertEquals("key", keyValueMaps.get(1).getKeyColumn());
    Assert.assertEquals("value2", keyValueMaps.get(1).getValueColumn());
    Assert.assertEquals(5L * 60_000L, namespace.getPollMs());
  }

  @Test
  public void testFlatDataNumeric()
  {
    final String keyField = "keyField";
    final String valueField = "valueField";
    final int n = 341879;
    final String nString = String.format("%d", n);
    URIExtractionNamespace.FlatDataParser parser = new URIExtractionNamespace.JSONFlatDataParser(
        new ObjectMapper(),
        ImmutableList.of(new KeyValueMap(mapName, keyField, valueField))
    ).withID(id);
    Assert.assertEquals(
        "num string value",
        ImmutableMap.of("B", nString),
        parser.parse(
            String.format(
                "{\"%s\":\"B\", \"%s\":\"%d\", \"FOO\":\"BAR\"}",
                keyField,
                valueField,
                n
            )
        ).get(new Pair(id, mapName))
    );
    Assert.assertEquals(
        "num string key",
        ImmutableMap.of(nString, "C"),
        parser.parse(
            String.format(
                "{\"%s\":\"%d\", \"%s\":\"C\", \"FOO\":\"BAR\"}",
                keyField,
                n,
                valueField
            )
        ).get(new Pair(id, mapName))
    );
    Assert.assertEquals(
        "num value",
        ImmutableMap.of("B", nString),
        parser.parse(
            String.format(
                "{\"%s\":\"B\", \"%s\":%d, \"FOO\":\"BAR\"}",
                keyField,
                valueField,
                n
            )
        ).get(new Pair(id, mapName))
    );
    Assert.assertEquals(
        "num key",
        ImmutableMap.of(nString, "C"),
        parser.parse(
            String.format(
                "{\"%s\":%d, \"%s\":\"C\", \"FOO\":\"BAR\"}",
                keyField,
                n,
                valueField
            )
        ).get(new Pair(id, mapName))
    );
  }

  @Test
  public void testSimpleJsonNumeric()
  {
    final URIExtractionNamespace.FlatDataParser parser = new URIExtractionNamespace.ObjectMapperFlatDataParser(
        registerTypes(new DefaultObjectMapper())
    ).withID(id);
    final int n = 341879;
    final String nString = String.format("%d", n);
    Assert.assertEquals(
        ImmutableMap.of("key", nString),
        parser.parse(String.format("{\"key\":%d}", n)).get(new Pair(id, KeyValueMap.DEFAULT_MAPNAME))
    );
  }
}
