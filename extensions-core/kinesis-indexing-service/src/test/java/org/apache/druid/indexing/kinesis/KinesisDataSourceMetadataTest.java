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


package org.apache.druid.indexing.kinesis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.initialization.CoreInjectorBuilder;
import org.apache.druid.initialization.DruidModule;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

public class KinesisDataSourceMetadataTest
{
  private static final KinesisDataSourceMetadata START0 = simpleStartMetadata(ImmutableMap.of());
  private static final KinesisDataSourceMetadata START1 = simpleStartMetadata(ImmutableMap.of("0", "2L", "1", "3L"));
  private static final KinesisDataSourceMetadata START2 = simpleStartMetadata(
      ImmutableMap.of("0", "2L", "1", "4L", "2", "5L")
  );
  private static final KinesisDataSourceMetadata START3 = simpleStartMetadata(ImmutableMap.of("0", "2L", "2", "5L"));
  private static final KinesisDataSourceMetadata START4 = startMetadata(
      ImmutableMap.of("0", "2L", "2", "5L"),
      ImmutableSet.of()
  );
  private static final KinesisDataSourceMetadata START5 = startMetadata(
      ImmutableMap.of("0", "2L", "1", "4L", "2", "5L"),
      ImmutableSet.of("0", "1")
  );
  private static final KinesisDataSourceMetadata END0 = endMetadata(ImmutableMap.of());
  private static final KinesisDataSourceMetadata END1 = endMetadata(ImmutableMap.of("0", "2L", "2", "5L"));
  private static final KinesisDataSourceMetadata END2 = endMetadata(ImmutableMap.of("0", "2L", "1", "4L"));

  @Test
  public void testMatches()
  {
    Assert.assertTrue(START0.matches(START0));
    Assert.assertTrue(START0.matches(START1));
    Assert.assertTrue(START0.matches(START2));
    Assert.assertTrue(START0.matches(START3));
    Assert.assertTrue(START0.matches(START4));
    Assert.assertTrue(START0.matches(START5));

    Assert.assertTrue(START1.matches(START0));
    Assert.assertTrue(START1.matches(START1));
    Assert.assertFalse(START1.matches(START2));
    Assert.assertTrue(START1.matches(START3));
    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(START1.matches(START5));

    Assert.assertTrue(START2.matches(START0));
    Assert.assertFalse(START2.matches(START1));
    Assert.assertTrue(START2.matches(START2));
    Assert.assertTrue(START2.matches(START3));
    Assert.assertFalse(START2.matches(START4));
    Assert.assertFalse(START2.matches(START5));

    Assert.assertTrue(START3.matches(START0));
    Assert.assertTrue(START3.matches(START1));
    Assert.assertTrue(START3.matches(START2));
    Assert.assertTrue(START3.matches(START3));
    Assert.assertFalse(START3.matches(START4));
    Assert.assertFalse(START3.matches(START5));

    Assert.assertTrue(START4.matches(START0));
    Assert.assertFalse(START4.matches(START1));
    Assert.assertFalse(START4.matches(START2));
    Assert.assertFalse(START4.matches(START3));
    Assert.assertTrue(START4.matches(START4));
    Assert.assertFalse(START4.matches(START5));

    Assert.assertTrue(START5.matches(START0));
    Assert.assertFalse(START5.matches(START1));
    Assert.assertFalse(START5.matches(START2));
    Assert.assertFalse(START5.matches(START3));
    Assert.assertFalse(START5.matches(START4));
    Assert.assertTrue(START5.matches(START5));

    Assert.assertTrue(END0.matches(END0));
    Assert.assertTrue(END0.matches(END1));
    Assert.assertTrue(END0.matches(END2));

    Assert.assertTrue(END1.matches(END0));
    Assert.assertTrue(END1.matches(END1));
    Assert.assertTrue(END1.matches(END2));

    Assert.assertTrue(END2.matches(END0));
    Assert.assertTrue(END2.matches(END1));
    Assert.assertTrue(END2.matches(END2));
  }

  @Test
  public void testIsValidStart()
  {
    Assert.assertTrue(START0.isValidStart());
    Assert.assertTrue(START1.isValidStart());
    Assert.assertTrue(START2.isValidStart());
    Assert.assertTrue(START3.isValidStart());
    Assert.assertTrue(START4.isValidStart());
    Assert.assertTrue(START5.isValidStart());
  }

  @Test
  public void testPlus()
  {
    Assert.assertEquals(
        simpleStartMetadata(ImmutableMap.of("0", "2L", "1", "3L", "2", "5L")),
        START1.plus(START3)
    );

    Assert.assertEquals(
        simpleStartMetadata(ImmutableMap.of("0", "2L", "1", "4L", "2", "5L")),
        START0.plus(START2)
    );

    Assert.assertEquals(
        simpleStartMetadata(ImmutableMap.of("0", "2L", "1", "4L", "2", "5L")),
        START1.plus(START2)
    );

    Assert.assertEquals(
        simpleStartMetadata(ImmutableMap.of("0", "2L", "1", "3L", "2", "5L")),
        START2.plus(START1)
    );

    Assert.assertEquals(
        simpleStartMetadata(ImmutableMap.of("0", "2L", "1", "4L", "2", "5L")),
        START2.plus(START2)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of("0", "2L", "1", "4L", "2", "5L"), ImmutableSet.of("1")),
        START2.plus(START4)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of("0", "2L", "1", "4L", "2", "5L"), ImmutableSet.of("0", "1")),
        START2.plus(START5)
    );

    Assert.assertEquals(
        endMetadata(ImmutableMap.of("0", "2L", "2", "5L")),
        END0.plus(END1)
    );

    Assert.assertEquals(
        endMetadata(ImmutableMap.of("0", "2L", "1", "4L", "2", "5L")),
        END1.plus(END2)
    );
  }

  @Test
  public void testMinus()
  {
    Assert.assertEquals(
        simpleStartMetadata(ImmutableMap.of("1", "3L")),
        START1.minus(START3)
    );

    Assert.assertEquals(
        simpleStartMetadata(ImmutableMap.of()),
        START0.minus(START2)
    );

    Assert.assertEquals(
        simpleStartMetadata(ImmutableMap.of()),
        START1.minus(START2)
    );

    Assert.assertEquals(
        simpleStartMetadata(ImmutableMap.of("2", "5L")),
        START2.minus(START1)
    );

    Assert.assertEquals(
        simpleStartMetadata(ImmutableMap.of()),
        START2.minus(START2)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of(), ImmutableSet.of()),
        START4.minus(START2)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of("1", "4L"), ImmutableSet.of("1")),
        START5.minus(START4)
    );

    Assert.assertEquals(
        endMetadata(ImmutableMap.of("1", "4L")),
        END2.minus(END1)
    );

    Assert.assertEquals(
        endMetadata(ImmutableMap.of("2", "5L")),
        END1.minus(END2)
    );
  }

  @Test
  public void testKinesisDataSourceMetadataSerdeRoundTrip() throws JsonProcessingException
  {
    ObjectMapper jsonMapper = createObjectMapper();

    KinesisDataSourceMetadata kdm1 = startMetadata(ImmutableMap.of(), ImmutableSet.of());
    String kdmStr1 = jsonMapper.writeValueAsString(kdm1);
    DataSourceMetadata dsMeta1 = jsonMapper.readValue(kdmStr1, DataSourceMetadata.class);
    Assert.assertEquals(kdm1, dsMeta1);

    KinesisDataSourceMetadata kdm2 = startMetadata(ImmutableMap.of("1", "3"), ImmutableSet.of());
    String kdmStr2 = jsonMapper.writeValueAsString(kdm2);
    DataSourceMetadata dsMeta2 = jsonMapper.readValue(kdmStr2, DataSourceMetadata.class);
    Assert.assertEquals(kdm2, dsMeta2);
  }

  @Test
  public void testKinesisDataSourceMetadataSerde() throws JsonProcessingException
  {
    ObjectMapper jsonMapper = createObjectMapper();
    KinesisDataSourceMetadata expectedKdm1 = endMetadata(ImmutableMap.of("1", "5"));
    String kdmStr1 = "{\"type\":\"kinesis\",\"partitions\":{\"type\":\"end\",\"stream\":\"foo\",\"topic\":\"foo\",\"partitionSequenceNumberMap\":{\"1\":5},\"partitionOffsetMap\":{\"1\":5},\"exclusivePartitions\":[]}}\n";
    DataSourceMetadata dsMeta1 = jsonMapper.readValue(kdmStr1, DataSourceMetadata.class);
    Assert.assertEquals(dsMeta1, expectedKdm1);

    KinesisDataSourceMetadata expectedKdm2 = endMetadata(ImmutableMap.of("1", "10", "2", "19"));
    String kdmStr2 = "{\"type\":\"kinesis\",\"partitions\":{\"type\":\"end\",\"stream\":\"foo\",\"topic\":\"food\",\"partitionSequenceNumberMap\":{\"1\":10, \"2\":19},\"partitionOffsetMap\":{\"1\":10, \"2\":19},\"exclusivePartitions\":[]}}\n";
    DataSourceMetadata dsMeta2 = jsonMapper.readValue(kdmStr2, DataSourceMetadata.class);
    Assert.assertEquals(dsMeta2, expectedKdm2);
  }

  private static KinesisDataSourceMetadata simpleStartMetadata(Map<String, String> sequences)
  {
    return startMetadata(sequences, sequences.keySet());
  }

  private static KinesisDataSourceMetadata startMetadata(Map<String, String> sequences, Set<String> exclusivePartitions)
  {
    return new KinesisDataSourceMetadata(
        new SeekableStreamStartSequenceNumbers<>("foo", sequences, exclusivePartitions)
    );
  }

  private static KinesisDataSourceMetadata endMetadata(Map<String, String> sequences)
  {
    return new KinesisDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>("foo", sequences));
  }

  private static ObjectMapper createObjectMapper()
  {
    DruidModule module = new KinesisIndexingServiceModule();
    final Injector injector = new CoreInjectorBuilder(new StartupInjectorBuilder().build())
        .addModule(
            binder -> {
              binder.bindConstant().annotatedWith(Names.named("serviceName")).to("test");
              binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8000);
              binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(9000);
            }
        ).build();
    ObjectMapper objectMapper = injector.getInstance(ObjectMapper.class);
    module.getJacksonModules().forEach(objectMapper::registerModule);
    return objectMapper;
  }
}
