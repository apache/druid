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

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * Bundle containing paths to generated TLS certificates and keystores.
 * Used for testing TLS/mTLS connectivity with services like Consul.
 */
public class TLSCertificateBundle
{
  private static final Logger log = new Logger(TLSCertificateBundle.class);

  private final Path certificateDirectory;
  private final Path trustStorePath;
  private final Path keyStorePath;

  public TLSCertificateBundle(Path certDir, Path trustStore, Path keyStore)
  {
    this.certificateDirectory = certDir;
    this.trustStorePath = trustStore;
    this.keyStorePath = keyStore;
  }

  /**
   * Returns the absolute path to the directory containing all certificate files.
   * This directory can be mounted to Docker containers.
   */
  public String getCertificateDirectory()
  {
    return certificateDirectory.toAbsolutePath().toString();
  }

  /**
   * Returns the absolute path to the PKCS12 truststore containing the CA certificate.
   * Use this to configure Java clients to trust the server certificate.
   */
  public String getTrustStorePath()
  {
    return trustStorePath.toAbsolutePath().toString();
  }

  /**
   * Returns the absolute path to the PKCS12 keystore containing the client certificate.
   * Use this to configure Java clients for mutual TLS (mTLS).
   */
  public String getKeyStorePath()
  {
    return keyStorePath.toAbsolutePath().toString();
  }

  /**
   * Returns the password for the truststore and keystore.
   * Always returns "changeit" for test certificates.
   */
  public String getStorePassword()
  {
    return "changeit";
  }

  /**
   * Cleans up the temporary directory containing all generated certificates.
   * Should be called after tests complete.
   */
  public void cleanup()
  {
    try {
      Files.walk(certificateDirectory)
           .sorted(Comparator.reverseOrder())
           .map(Path::toFile)
           .forEach(File::delete);
      log.debug("Cleaned up certificate directory: %s", certificateDirectory);
    }
    catch (IOException e) {
      log.warn(e, "Failed to clean up certificate directory: %s", certificateDirectory);
    }
  }

  /**
   * Verifies that all required files exist in the bundle.
   *
   * @throws ISE if any required files are missing
   */
  public void validate()
  {
    if (!Files.exists(certificateDirectory)) {
      throw new ISE("Certificate directory does not exist: %s", certificateDirectory);
    }
    if (!Files.exists(trustStorePath)) {
      throw new ISE("Truststore does not exist: %s", trustStorePath);
    }
    if (!Files.exists(keyStorePath)) {
      throw new ISE("Keystore does not exist: %s", keyStorePath);
    }
  }
}
