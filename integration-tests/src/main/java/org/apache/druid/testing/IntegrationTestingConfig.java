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

package org.apache.druid.testing;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Configuration for tests. Opinionated about the shape of the cluster:
 * there is one or two Coordinators or Overlords, zero or one of
 * everything else.
 * <p>
 * To work in Docker (and K8s) there are two methods per host:
 * {@code get<service>Host()} which returns the host as seen from
 * the test machine (meaning the proxy host), and
 * {@code get<service>InternalHost()} which returns the name of
 * the host as seen by itself and other services: the host published
 * in ZK, which is the host known to the Docker/K8s overlay network.
 * <p>
 * The {@code get<service>Url()} methods return URLs relative to
 * the test, using the proxy host for Docker and K8s.
 */
public interface IntegrationTestingConfig
{
  String getZookeeperHosts();

  default String getZookeeperInternalHosts()
  {
    return getZookeeperHosts();
  }

  String getKafkaHost();

  default String getKafkaInternalHost()
  {
    return getKafkaHost();
  }

  String getBrokerHost();

  default String getBrokerInternalHost()
  {
    return getBrokerHost();
  }

  String getRouterHost();

  default String getRouterInternalHost()
  {
    return getRouterHost();
  }

  String getCoordinatorHost();

  default String getCoordinatorInternalHost()
  {
    return getCoordinatorHost();
  }

  String getCoordinatorTwoHost();

  default String getCoordinatorTwoInternalHost()
  {
    return getCoordinatorTwoHost();
  }

  String getOverlordHost();

  default String getOverlordInternalHost()
  {
    return getOverlordHost();
  }

  String getOverlordTwoHost();

  default String getOverlordTwoInternalHost()
  {
    return getOverlordTwoHost();
  }

  String getMiddleManagerHost();

  default String getMiddleManagerInternalHost()
  {
    return getMiddleManagerHost();
  }

  String getHistoricalHost();

  default String getHistoricalInternalHost()
  {
    return getHistoricalHost();
  }

  String getCoordinatorUrl();

  String getCoordinatorTLSUrl();

  String getCoordinatorTwoUrl();

  String getCoordinatorTwoTLSUrl();

  String getOverlordUrl();

  String getOverlordTLSUrl();

  String getOverlordTwoUrl();

  String getOverlordTwoTLSUrl();

  String getIndexerUrl();

  String getIndexerTLSUrl();

  String getRouterUrl();

  String getRouterTLSUrl();

  String getPermissiveRouterUrl();

  String getPermissiveRouterTLSUrl();

  String getNoClientAuthRouterUrl();

  String getNoClientAuthRouterTLSUrl();

  String getCustomCertCheckRouterUrl();

  String getCustomCertCheckRouterTLSUrl();

  String getBrokerUrl();

  String getBrokerTLSUrl();

  String getHistoricalUrl();

  String getHistoricalTLSUrl();

  String getProperty(String prop);

  String getUsername();

  String getPassword();

  Map<String, String> getProperties();

  boolean manageKafkaTopic();

  String getExtraDatasourceNameSuffix();

  String getCloudBucket();

  String getCloudPath();

  String getCloudRegion();

  String getS3AssumeRoleWithExternalId();

  String getS3AssumeRoleExternalId();

  String getS3AssumeRoleWithoutExternalId();

  String getAzureKey();

  String getHadoopGcsCredentialsPath();

  String getStreamEndpoint();

  String getSchemaRegistryHost();

  default String getSchemaRegistryInternalHost()
  {
    return getSchemaRegistryHost();
  }

  boolean isDocker();

  @Nullable
  default String getDockerHost()
  {
    return null;
  }
}
