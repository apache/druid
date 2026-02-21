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

package org.apache.druid.testing.embedded.auth;

import org.apache.commons.io.FileUtils;
import org.apache.druid.https.SSLContextModule;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedResource;

import java.io.File;
import java.io.IOException;

/**
 * {@link EmbeddedResource} used to enable SSL on Druid services.
 * This resource is responsible for the following:
 * <ul>
 * <li>Generate client certificates using script
 * {@code integration-tests/docker/tls/generate-client-certs-and-keystores.sh}</li>
 * <li>Generate server truststore using script
 * {@code integration-tests/docker/tls/generate-server-certs-and-keystores.sh}</li>
 * <li>Keeps the generated certificates in the {@code TestFolder} used by the cluster</li>
 * <li>Configures the cluster with the appropriate keystore/truststore paths.</li>
 * </ul>
 */
public class EmbeddedSSLAuthResource implements EmbeddedResource
{
  private EmbeddedDruidCluster cluster;

  @Override
  public void beforeStart(EmbeddedDruidCluster cluster)
  {
    this.cluster = cluster;
  }

  @Override
  public void start() throws Exception
  {
    final File tlsDir = getTlsTempDirectory();

    final File tlsScripts = new File(tlsDir, "scripts");
    copyScriptsToDirectory(tlsScripts);

    // Generate client certificates and keystores
    final ProcessBuilder generateClientCertificates = new ProcessBuilder(
        "bash",
        new File(tlsScripts, "generate-client-certs-and-keystores.sh").getAbsolutePath()
    );
    generateClientCertificates.directory(tlsDir);
    generateClientCertificates.redirectErrorStream(true);
    generateClientCertificates.redirectOutput(ProcessBuilder.Redirect.INHERIT);

    int exitCode = generateClientCertificates.start().waitFor();
    if (exitCode != 0) {
      throw new ISE("Client certificate generation failed");
    }

    // Generate server certificates and keystores
    final ProcessBuilder generateServerCertificates = new ProcessBuilder(
        "bash",
        new File(tlsScripts, "generate-server-certs-and-keystores.sh").getAbsolutePath()
    );
    generateServerCertificates.directory(tlsDir);
    generateServerCertificates.redirectErrorStream(true);
    generateServerCertificates.redirectOutput(ProcessBuilder.Redirect.INHERIT);

    exitCode = generateServerCertificates.start().waitFor();
    if (exitCode != 0) {
      throw new ISE("Server certificate generation failed");
    }
  }

  private void copyScriptsToDirectory(File targetDir)
  {
    try {
      FileUtils.copyDirectory(new File("../integration-tests/docker/tls"), targetDir);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onStarted(EmbeddedDruidCluster cluster)
  {
    final String truststore = getTlsFilePath("server_tls/truststore.jks");
    final String keystore = getTlsFilePath("server_tls/server.p12");

    cluster.addExtension(SSLContextModule.class)
           .addCommonProperty("druid.enableTlsPort", "true")

           .addCommonProperty("druid.server.https.certAlias", "druid")
           .addCommonProperty("druid.server.https.keyManagerPassword", "druid123")
           .addCommonProperty("druid.server.https.keyStorePassword", "druid123")
           .addCommonProperty("druid.server.https.keyStorePath", keystore)
           .addCommonProperty("druid.server.https.keyStoreType", "PKCS12")
           .addCommonProperty("druid.server.https.requireClientCertificate", "true")
           .addCommonProperty("druid.server.https.trustStoreAlgorithm", "PKIX")
           .addCommonProperty("druid.server.https.trustStorePassword", "druid123")
           .addCommonProperty("druid.server.https.trustStorePath", truststore)
           .addCommonProperty("druid.server.https.validateHostnames", "true")

           .addCommonProperty("druid.client.https.protocol", "TLSv1.2")
           .addCommonProperty("druid.client.https.certAlias", "druid")
           .addCommonProperty("druid.client.https.keyManagerPassword", "druid123")
           .addCommonProperty("druid.client.https.keyStorePassword", "druid123")
           .addCommonProperty("druid.client.https.keyStorePath", keystore)
           .addCommonProperty("druid.client.https.trustStoreAlgorithm", "PKIX")
           .addCommonProperty("druid.client.https.trustStorePassword", "druid123")
           .addCommonProperty("druid.client.https.trustStorePath", truststore);
  }

  @Override
  public void stop() throws Exception
  {
    // do nothing
  }

  /**
   * @return Absolute path of the given file inside the temporary TLS directory
   * used by this resource.
   */
  public String getTlsFilePath(String filename)
  {
    return new File(getTlsTempDirectory(), filename).getAbsolutePath();
  }

  private File getTlsTempDirectory()
  {
    return cluster.getTestFolder().getOrCreateFolder("tls");
  }
}
