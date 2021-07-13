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

package org.apache.druid.testing.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.netty.NettyDockerCmdExecFactory;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.TestClient;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class DockerDruidClusterAdminClient extends AbstractDruidClusterAdminClient
{
  private static final Logger LOG = new Logger(DockerDruidClusterAdminClient.class);
  private static final String COORDINATOR_DOCKER_CONTAINER_NAME = "/druid-coordinator";
  private static final String COORDINATOR_TWO_DOCKER_CONTAINER_NAME = "/druid-coordinator-two";
  private static final String HISTORICAL_DOCKER_CONTAINER_NAME = "/druid-historical";
  private static final String OVERLORD_DOCKER_CONTAINER_NAME = "/druid-overlord";
  private static final String OVERLORD_TWO_DOCKER_CONTAINER_NAME = "/druid-overlord-two";
  private static final String BROKER_DOCKER_CONTAINER_NAME = "/druid-broker";
  private static final String ROUTER_DOCKER_CONTAINER_NAME = "/druid-router";
  private static final String MIDDLEMANAGER_DOCKER_CONTAINER_NAME = "/druid-middlemanager";

  @Inject
  public DockerDruidClusterAdminClient(
      ObjectMapper jsonMapper,
      @TestClient HttpClient httpClient,
      IntegrationTestingConfig config
  )
  {
    super(jsonMapper, httpClient, config);
  }

  @Override
  public void restartCoordinatorContainer()
  {
    restartDockerContainer(COORDINATOR_DOCKER_CONTAINER_NAME);
  }

  @Override
  public void restartCoordinatorTwoContainer()
  {
    restartDockerContainer(COORDINATOR_TWO_DOCKER_CONTAINER_NAME);
  }

  @Override
  public void restartHistoricalContainer()
  {
    restartDockerContainer(HISTORICAL_DOCKER_CONTAINER_NAME);
  }

  @Override
  public void restartOverlordContainer()
  {
    restartDockerContainer(OVERLORD_DOCKER_CONTAINER_NAME);
  }

  @Override
  public void restartOverlordTwoContainer()
  {
    restartDockerContainer(OVERLORD_TWO_DOCKER_CONTAINER_NAME);
  }

  @Override
  public void restartBrokerContainer()
  {
    restartDockerContainer(BROKER_DOCKER_CONTAINER_NAME);
  }

  @Override
  public void restartRouterContainer()
  {
    restartDockerContainer(ROUTER_DOCKER_CONTAINER_NAME);
  }

  @Override
  public void restartMiddleManagerContainer()
  {
    restartDockerContainer(MIDDLEMANAGER_DOCKER_CONTAINER_NAME);
  }

  private void restartDockerContainer(String serviceName)
  {
    DockerClient dockerClient = DockerClientBuilder.getInstance()
                                                   .withDockerCmdExecFactory((new NettyDockerCmdExecFactory())
                                                                                 .withConnectTimeout(10 * 1000))
                                                   .build();
    List<Container> containers = dockerClient.listContainersCmd().exec();
    Optional<String> containerName = containers.stream()
                                               .filter(container -> Arrays.asList(container.getNames()).contains(serviceName))
                                               .findFirst()
                                               .map(container -> container.getId());

    if (!containerName.isPresent()) {
      LOG.error("Cannot find docker container for " + serviceName);
      throw new ISE("Cannot find docker container for " + serviceName);
    }
    dockerClient.restartContainerCmd(containerName.get()).exec();
  }
}
