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
package io.druid.data.input.avro;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import io.druid.data.input.AvroHadoopInputRowParser;
import io.druid.data.input.AvroStreamInputRowParser;
import io.druid.data.input.schemarepo.Avro1124RESTRepositoryClientWrapper;
import io.druid.initialization.DruidModule;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.Repository;
import org.schemarepo.ValidatorFactory;
import org.schemarepo.json.GsonJsonUtil;
import org.schemarepo.json.JsonUtil;

import java.util.Collections;
import java.util.List;

public class AvroExtensionsModule implements DruidModule
{
  public AvroExtensionsModule() {}

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(
        new SimpleModule("AvroInputRowParserModule")
            .registerSubtypes(
                new NamedType(AvroStreamInputRowParser.class, "avro_stream"),
                new NamedType(AvroHadoopInputRowParser.class, "avro_hadoop")
            )
            .setMixInAnnotation(Repository.class, RepositoryMixIn.class)
            .setMixInAnnotation(JsonUtil.class, JsonUtilMixIn.class)
            .setMixInAnnotation(InMemoryRepository.class, InMemoryRepositoryMixIn.class)
    );
  }

  @Override
  public void configure(Binder binder)
  { }
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = GsonJsonUtil.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "gson", value = GsonJsonUtil.class)
})
abstract class JsonUtilMixIn
{
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = Avro1124RESTRepositoryClientWrapper.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "avro_1124_rest_client", value = Avro1124RESTRepositoryClientWrapper.class),
    @JsonSubTypes.Type(name = "in_memory_for_unit_test", value = InMemoryRepository.class)
})
abstract class RepositoryMixIn
{
}

abstract class InMemoryRepositoryMixIn
{
  @JsonCreator
  public InMemoryRepositoryMixIn(@JsonProperty("validators") ValidatorFactory validators)
  {
  }
}

