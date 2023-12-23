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

package org.apache.druid.indexing.kafka;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.indexing.overlord.sampler.InputSourceSampler;
import org.apache.druid.indexing.overlord.sampler.SamplerConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamSamplerSpec;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class KafkaSamplerSpec extends SeekableStreamSamplerSpec
{
  private final ObjectMapper objectMapper;

  @JsonCreator
  public KafkaSamplerSpec(
      @JsonProperty("spec") final KafkaSupervisorSpec ingestionSpec,
      @JsonProperty("samplerConfig") @Nullable final SamplerConfig samplerConfig,
      @JacksonInject InputSourceSampler inputSourceSampler,
      @JacksonInject ObjectMapper objectMapper
  )
  {
    super(ingestionSpec, samplerConfig, inputSourceSampler);

    this.objectMapper = objectMapper;
  }

  @Override
  protected KafkaRecordSupplier createRecordSupplier()
  {
    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

      final Map<String, Object> props = new HashMap<>(((KafkaSupervisorIOConfig) ioConfig).getConsumerProperties());

      props.put("enable.auto.commit", "false");
      props.put("auto.offset.reset", "none");
      props.put("request.timeout.ms", Integer.toString(samplerConfig.getTimeoutMs()));
      KafkaSupervisorIOConfig kafkaSupervisorIOConfig = (KafkaSupervisorIOConfig) ioConfig;

      return new KafkaRecordSupplier(props, objectMapper, kafkaSupervisorIOConfig.getConfigOverrides(),
                                     kafkaSupervisorIOConfig.isMultiTopic()
      );
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

  @Override
  public String getType()
  {
    return KafkaIndexTaskModule.SCHEME;
  }

  @Override
  @JsonIgnore
  @Nonnull
  public Set<ResourceAction> getInputSourceResources() throws UOE
  {
    return Collections.singleton(new ResourceAction(
        new Resource(KafkaIndexTaskModule.SCHEME, ResourceType.EXTERNAL),
        Action.READ
    ));
  }
}
