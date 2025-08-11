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

package org.apache.druid.testing.embedded.indexing;

import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.druid.testing.embedded.TestcontainerResource;
import org.testcontainers.containers.GenericContainer;

/**
 * A resource for managing a Schema Registry instance in embedded tests.
 */
public class KafkaSchemaRegistryResource extends TestcontainerResource<GenericContainer<?>>
{
  private static final String SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry:latest";

  KafkaResource kafkaResource;

  KafkaSchemaRegistryResource(KafkaResource kafkaResource)
  {
    super();
    this.kafkaResource = kafkaResource;
  }

  @Override
  protected GenericContainer<?> createContainer()
  {
    return new GenericContainer<>(SCHEMA_REGISTRY_IMAGE)
        .dependsOn(kafkaResource.getContainer())
        .withExposedPorts(9081)
        .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
        .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:9081")
        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", kafkaResource.getBootstrapServerUrl());
  }
}
