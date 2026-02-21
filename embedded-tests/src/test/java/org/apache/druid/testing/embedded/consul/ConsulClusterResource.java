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

package org.apache.druid.testing.embedded.consul;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.TestcontainerResource;
import org.apache.druid.testing.utils.TLSCertificateBundle;
import org.apache.druid.testing.utils.TLSCertificateGenerator;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.Nullable;
import java.net.URI;
import java.time.Duration;

/**
 * Runs a single-node Consul agent for use as the discovery backend during docker tests.
 * Supports plain HTTP, TLS, and mutual TLS (mTLS) modes.
 */
public class ConsulClusterResource extends TestcontainerResource<GenericContainer<?>>
{
  private static final Logger log = new Logger(ConsulClusterResource.class);
  private static final int CONSUL_HTTP_PORT = 8500;
  private static final int CONSUL_HTTPS_PORT = 8501;
  private static final DockerImageName CONSUL_IMAGE = DockerImageName.parse("hashicorp/consul:1.18");

  private final ConsulSecurityMode securityMode;
  private String consulHostForDruid;
  private int consulPortForDruid;

  @Nullable
  private TLSCertificateBundle certBundle;

  /**
   * Creates a Consul cluster resource with plain HTTP (no encryption).
   */
  public ConsulClusterResource()
  {
    this(ConsulSecurityMode.PLAIN);
  }

  /**
   * Creates a Consul cluster resource with the specified security mode.
   *
   * @param securityMode security mode (PLAIN, TLS, or MTLS)
   */
  public ConsulClusterResource(ConsulSecurityMode securityMode)
  {
    this.securityMode = securityMode;
  }

  @Override
  protected GenericContainer<?> createContainer()
  {
    try {
      if (securityMode == ConsulSecurityMode.TLS || securityMode == ConsulSecurityMode.MTLS) {
        certBundle = TLSCertificateGenerator.generateToTempDirectory();
        log.info("Generated TLS certificates for Consul in: %s", certBundle.getCertificateDirectory());
      }

      GenericContainer<?> container = new GenericContainer<>(CONSUL_IMAGE);

      if (securityMode == ConsulSecurityMode.PLAIN) {
        container
            .withCommand("agent", "-server", "-bootstrap-expect=1", "-client=0.0.0.0", "-bind=0.0.0.0", "-ui", "-datacenter=dc1")
            .withExposedPorts(CONSUL_HTTP_PORT)
            .waitingFor(Wait.forHttp("/v1/status/leader").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(2)));
      } else {
        String configFile = securityMode == ConsulSecurityMode.TLS
                            ? "consul-config-tls-only.json"
                            : "consul-config-mtls.json";

        container
            .withCommand("agent", "-server", "-bootstrap-expect=1", "-client=0.0.0.0", "-bind=0.0.0.0", "-config-file=/consul/config/" + configFile)
            .withExposedPorts(CONSUL_HTTPS_PORT, CONSUL_HTTP_PORT)
            .withFileSystemBind(certBundle.getCertificateDirectory(), "/tls", BindMode.READ_ONLY)
            .withClasspathResourceMapping("tls/" + configFile, "/consul/config/" + configFile, BindMode.READ_ONLY)
            .waitingFor(Wait.forLogMessage(".*agent: Consul agent running!.*", 1).withStartupTimeout(Duration.ofMinutes(2)));
      }

      return container;
    }
    catch (Exception e) {
      throw new RuntimeException("Failed to create Consul container", e);
    }
  }

  @Override
  public void onStarted(EmbeddedDruidCluster cluster)
  {
    consulHostForDruid = getContainer().getContainerInfo().getNetworkSettings().getIpAddress();
    consulPortForDruid = securityMode == ConsulSecurityMode.PLAIN ? CONSUL_HTTP_PORT : CONSUL_HTTPS_PORT;

    cluster.addCommonProperty("druid.discovery.type", "consul");
    cluster.addCommonProperty("druid.discovery.consul.service.servicePrefix", "druid");
  }

  @Override
  public void stop()
  {
    super.stop();
    if (certBundle != null) {
      certBundle.cleanup();
      certBundle = null;
    }
  }

  public String getConsulHostForDruid()
  {
    return consulHostForDruid;
  }

  public int getConsulPortForDruid()
  {
    return consulPortForDruid;
  }

  public int getMappedPort()
  {
    ensureRunning();
    int internalPort = securityMode == ConsulSecurityMode.PLAIN ? CONSUL_HTTP_PORT : CONSUL_HTTPS_PORT;
    return getContainer().getMappedPort(internalPort);
  }

  public URI getHttpUri(String pathAndQuery)
  {
    ensureRunning();
    String normalizedPath = pathAndQuery.startsWith("/") ? pathAndQuery : "/" + pathAndQuery;
    String scheme = securityMode == ConsulSecurityMode.PLAIN ? "http" : "https";
    int internalPort = securityMode == ConsulSecurityMode.PLAIN ? CONSUL_HTTP_PORT : CONSUL_HTTPS_PORT;
    return URI.create(StringUtils.format("%s://%s:%d%s", scheme, getContainer().getHost(), getContainer().getMappedPort(internalPort), normalizedPath));
  }

  public ConsulSecurityMode getSecurityMode()
  {
    return securityMode;
  }

  @Nullable
  public TLSCertificateBundle getCertificateBundle()
  {
    return certBundle;
  }

  @Nullable
  public String getTrustStorePath()
  {
    return certBundle != null ? certBundle.getTrustStorePath() : null;
  }

  @Nullable
  public String getKeyStorePath()
  {
    return certBundle != null ? certBundle.getKeyStorePath() : null;
  }

  public String getStorePassword()
  {
    return "changeit";
  }

}
