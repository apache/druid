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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.TestcontainerResource;
import org.testcontainers.redpanda.RedpandaContainer;

/**
 * A resource for managing a Schema Registry instance in embedded tests.
 * <p>
 * Uses Redpanda testcontainers under the hood which offers built in schema registry support that is confluent
 * compatible while having a much smaller image size than the official confluent schema registry image.
 */
public class KafkaSchemaRegistryResource extends TestcontainerResource<RedpandaContainer>
{
  private static final String SCHEMA_REGISTRY_IMAGE = "docker.redpanda.com/redpandadata/redpanda:v25.2.2";
  private static final int SCHEMA_REGISTRY_PORT = 8081;

  private final KafkaResource kafkaResource;
  private String connectURI;
  private String embeddedHostname;

  KafkaSchemaRegistryResource(KafkaResource kafkaResource)
  {
    super();
    this.kafkaResource = kafkaResource;
  }

  @Override
  protected RedpandaContainer createContainer()
  {
    return new RedpandaContainer(SCHEMA_REGISTRY_IMAGE)
        .dependsOn(kafkaResource.getContainer());
  }

  @Override
  public void onStarted(EmbeddedDruidCluster cluster)
  {
    connectURI = createConnectURI(cluster);
    embeddedHostname = cluster.getEmbeddedHostname().toString();
  }

  public String getHostandPort()
  {
    ensureRunning();
    return StringUtils.format(
        "%s:%d",
        embeddedHostname,
        getContainer().getMappedPort(SCHEMA_REGISTRY_PORT)
    );
  }

  public String getConnectURI()
  {
    ensureRunning();
    return connectURI;
  }

  private String createConnectURI(EmbeddedDruidCluster cluster)
  {
    ensureRunning();
    return StringUtils.format(
        "http://%s:%d",
        cluster.getEmbeddedHostname(),
        getContainer().getMappedPort(SCHEMA_REGISTRY_PORT)
    );
  }
}
