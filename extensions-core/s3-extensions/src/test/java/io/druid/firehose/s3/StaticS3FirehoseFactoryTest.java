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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.guice.ObjectMapperModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import io.druid.initialization.DruidModule;
import io.druid.jackson.DefaultObjectMapper;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

/**
 */
public class StaticS3FirehoseFactoryTest
{
  private static final RestS3Service SERVICE = new RestS3Service(null);

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
    final ObjectMapper baseMapper = new DefaultObjectMapper();
    baseModule.getJacksonModules().forEach(baseMapper::registerModule);

    final Injector injector = Guice.createInjector(
        new ObjectMapperModule(),
        baseModule
    );
    return injector.getInstance(ObjectMapper.class);
  }

  private static class TestS3Module implements DruidModule
  {
    @Override
    public List<? extends Module> getJacksonModules()
    {
      return ImmutableList.of(new SimpleModule());
    }

    @Override
    public void configure(Binder binder)
    {

    }

    @Provides
    public RestS3Service getRestS3Service()
    {
      return SERVICE;
    }
  }
}
