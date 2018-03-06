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

package io.druid.firehose.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.guice.ObjectMapperModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import io.druid.initialization.DruidModule;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

/**
 */
public class StaticS3FirehoseFactoryTest
{
  private static final AmazonS3Client SERVICE = new AmazonS3Client();

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper mapper = createObjectMapper(new TestS3Module());

    final List<URI> uris = Arrays.asList(
        new URI("s3://foo/bar/file.gz"),
        new URI("s3://bar/foo/file2.gz")
    );

    final StaticS3FirehoseFactory factory = new StaticS3FirehoseFactory(
        SERVICE,
        uris,
        null,
        2048L,
        1024L,
        512L,
        100L,
        5
    );

    final StaticS3FirehoseFactory outputFact = mapper.readValue(
        mapper.writeValueAsString(factory),
        StaticS3FirehoseFactory.class
    );

    Assert.assertEquals(factory, outputFact);
  }

  private static ObjectMapper createObjectMapper(DruidModule baseModule)
  {
    final Injector injector = Guice.createInjector(
        new ObjectMapperModule(),
        baseModule
    );
    final ObjectMapper baseMapper = injector.getInstance(ObjectMapper.class);

    baseModule.getJacksonModules().forEach(baseMapper::registerModule);
    return baseMapper;
  }

  private static class TestS3Module implements DruidModule
  {
    @Override
    public List<? extends Module> getJacksonModules()
    {
      // Deserializer is need for AmazonS3Client even though it is injected.
      // See https://github.com/FasterXML/jackson-databind/issues/962.
      return ImmutableList.of(new SimpleModule().addDeserializer(AmazonS3.class, new ItemDeserializer()));
    }

    @Override
    public void configure(Binder binder)
    {

    }

    @Provides
    public AmazonS3 getAmazonS3Client()
    {
      return SERVICE;
    }
  }

  public static class ItemDeserializer extends StdDeserializer<AmazonS3>
  {
    public ItemDeserializer()
    {
      this(null);
    }

    public ItemDeserializer(Class<?> vc)
    {
      super(vc);
    }

    @Override
    public AmazonS3 deserialize(JsonParser jp, DeserializationContext ctxt)
    {
      throw new UnsupportedOperationException();
    }
  }
}
