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

package io.druid.java.util.emitter.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.emitter.factory.EmitterFactory;
import org.asynchttpclient.AsyncHttpClient;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

public class CustomEmitterFactoryTest
{
  @JsonTypeName("test")
  public static class TestEmitterConfig implements EmitterFactory
  {
    @JsonProperty
    private String stringProperty;
    @JsonProperty
    private int intProperty;

    @Override
    public Emitter makeEmitter(ObjectMapper objectMapper, AsyncHttpClient httpClient, Lifecycle lifecycle)
    {
      return new StubEmitter(stringProperty, intProperty);
    }
  }

  public static class StubEmitter implements Emitter
  {
    private String stringProperty;
    private int intProperty;

    public StubEmitter(String stringProperty, int intProperty)
    {
      this.stringProperty = stringProperty;
      this.intProperty = intProperty;
    }

    public String getStringProperty()
    {
      return stringProperty;
    }

    public int getIntProperty()
    {
      return intProperty;
    }

    @Override
    public void start() {}

    @Override
    public void emit(Event event) {}

    @Override
    public void flush() throws IOException {}

    @Override
    public void close() throws IOException {}
  }

  @Test
  public void testCustomEmitter()
  {
    final Properties props = new Properties();
    props.put("io.druid.java.util.emitter.stringProperty", "http://example.com/");
    props.put("io.druid.java.util.emitter.intProperty", "1");
    props.put("io.druid.java.util.emitter.type", "test");

    final ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerSubtypes(TestEmitterConfig.class);
    final Lifecycle lifecycle = new Lifecycle();
    final Emitter emitter = Emitters.create(props, null, objectMapper, lifecycle);

    Assert.assertTrue("created emitter should be of class StubEmitter", emitter instanceof StubEmitter);
    StubEmitter stubEmitter = (StubEmitter) emitter;
    Assert.assertEquals("http://example.com/", stubEmitter.getStringProperty());
    Assert.assertEquals(1, stubEmitter.getIntProperty());
  }
}
