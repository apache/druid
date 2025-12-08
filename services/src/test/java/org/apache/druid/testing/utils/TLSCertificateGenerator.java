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

import org.apache.druid.java.util.common.FileUtils;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Date;

/**
 * Utility class for generating TLS certificates and keystores for testing.
 * Generates self-signed CA certificates, server certificates with SANs,
 * and client certificates for mutual TLS.
 */
public class TLSCertificateGenerator
{
  private static final String SIGNATURE_ALGORITHM = "SHA256withRSA";
  private static final int KEY_SIZE = 2048;
  private static final long VALIDITY_DAYS = 365;
  private static final String STORE_PASSWORD = "changeit";

  static {
    // Register Bouncy Castle as a security provider
    if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
      Security.addProvider(new BouncyCastleProvider());
    }
  }

  /**
   * Generates a complete set of TLS certificates to a temporary directory.
   * The directory will contain:
   * - CA certificate and key (PEM format)
   * - Server certificate and key with SAN for localhost (PEM format)
   * - Client certificate and key for mTLS (PEM format)
   * - Java truststore (PKCS12) with CA certificate
   * - Java keystore (PKCS12) with client certificate
   *
   * Caller is responsible for cleanup via {@link TLSCertificateBundle#cleanup()}.
   *
   * @return bundle containing paths to all generated files
   * @throws Exception if certificate generation fails
   */
  public static TLSCertificateBundle generateToTempDirectory() throws Exception
  {
    Path tempDir = FileUtils.createTempDir().toPath();
    return generateToDirectory(tempDir);
  }

  /**
   * Generates a complete set of TLS certificates to the specified directory.
   *
   * @param directory target directory for certificate files
   * @return bundle containing paths to all generated files
   * @throws Exception if certificate generation fails
   */
  public static TLSCertificateBundle generateToDirectory(Path directory) throws Exception
  {
    // Generate CA certificate and key
    KeyPair caKeyPair = generateKeyPair();
    X509Certificate caCert = generateCACertificate(caKeyPair);

    // Generate server certificate with SAN for localhost
    KeyPair serverKeyPair = generateKeyPair();
    X509Certificate serverCert = generateServerCertificate(
        serverKeyPair,
        caCert,
        caKeyPair.getPrivate(),
        "localhost"
    );

    // Generate client certificate for mTLS
    KeyPair clientKeyPair = generateKeyPair();
    X509Certificate clientCert = generateClientCertificate(
        clientKeyPair,
        caCert,
        caKeyPair.getPrivate(),
        "druid-client"
    );

    // Write PEM files (for Consul and other services)
    writePEM(directory.resolve("ca-cert.pem"), "CERTIFICATE", caCert.getEncoded());
    writePEM(directory.resolve("ca-key.pem"), "PRIVATE KEY", caKeyPair.getPrivate().getEncoded());
    writePEM(directory.resolve("consul-server-cert.pem"), "CERTIFICATE", serverCert.getEncoded());
    writePEM(directory.resolve("consul-server-key.pem"), "PRIVATE KEY", serverKeyPair.getPrivate().getEncoded());
    writePEM(directory.resolve("client-cert.pem"), "CERTIFICATE", clientCert.getEncoded());
    writePEM(directory.resolve("client-key.pem"), "PRIVATE KEY", clientKeyPair.getPrivate().getEncoded());

    // Create Java keystores (for Druid clients)
    Path trustStore = createTrustStore(directory, caCert);
    Path keyStore = createKeyStore(directory, clientCert, clientKeyPair.getPrivate());

    return new TLSCertificateBundle(directory, trustStore, keyStore);
  }

  private static KeyPair generateKeyPair() throws Exception
  {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
    keyGen.initialize(KEY_SIZE, new SecureRandom());
    return keyGen.generateKeyPair();
  }

  private static X509Certificate generateCACertificate(KeyPair keyPair) throws Exception
  {
    X500Name issuer = new X500Name("CN=Test CA, O=Apache Druid Test, C=US");
    BigInteger serial = BigInteger.valueOf(System.currentTimeMillis());
    Date notBefore = new Date();
    Date notAfter = new Date(notBefore.getTime() + VALIDITY_DAYS * 24 * 60 * 60 * 1000L);

    X509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(
        issuer,       // issuer
        serial,       // serial
        notBefore,    // notBefore
        notAfter,     // notAfter
        issuer,       // subject (self-signed)
        keyPair.getPublic()
    );

    // Mark as CA certificate
    certBuilder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));

    ContentSigner signer = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM)
        .setProvider(BouncyCastleProvider.PROVIDER_NAME)
        .build(keyPair.getPrivate());

    return new JcaX509CertificateConverter()
        .setProvider(BouncyCastleProvider.PROVIDER_NAME)
        .getCertificate(certBuilder.build(signer));
  }

  private static X509Certificate generateServerCertificate(
      KeyPair keyPair,
      X509Certificate caCert,
      PrivateKey caKey,
      String hostname
  ) throws Exception
  {
    X500Name issuer = new X500Name(caCert.getSubjectX500Principal().getName());
    X500Name subject = new X500Name("CN=" + hostname + ", O=Apache Druid Test, C=US");
    BigInteger serial = BigInteger.valueOf(System.currentTimeMillis());
    Date notBefore = new Date();
    Date notAfter = new Date(notBefore.getTime() + VALIDITY_DAYS * 24 * 60 * 60 * 1000L);

    X509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(
        issuer,
        serial,
        notBefore,
        notAfter,
        subject,
        keyPair.getPublic()
    );

    // Add Subject Alternative Names for localhost
    GeneralName[] altNames = new GeneralName[]{
        new GeneralName(GeneralName.dNSName, hostname),
        new GeneralName(GeneralName.iPAddress, "127.0.0.1")
    };
    certBuilder.addExtension(
        Extension.subjectAlternativeName,
        false,
        new GeneralNames(altNames)
    );

    ContentSigner signer = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM)
        .setProvider(BouncyCastleProvider.PROVIDER_NAME)
        .build(caKey);

    return new JcaX509CertificateConverter()
        .setProvider(BouncyCastleProvider.PROVIDER_NAME)
        .getCertificate(certBuilder.build(signer));
  }

  private static X509Certificate generateClientCertificate(
      KeyPair keyPair,
      X509Certificate caCert,
      PrivateKey caKey,
      String commonName
  ) throws Exception
  {
    X500Name issuer = new X500Name(caCert.getSubjectX500Principal().getName());
    X500Name subject = new X500Name("CN=" + commonName + ", O=Apache Druid Test, C=US");
    BigInteger serial = BigInteger.valueOf(System.currentTimeMillis());
    Date notBefore = new Date();
    Date notAfter = new Date(notBefore.getTime() + VALIDITY_DAYS * 24 * 60 * 60 * 1000L);

    X509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(
        issuer,
        serial,
        notBefore,
        notAfter,
        subject,
        keyPair.getPublic()
    );

    ContentSigner signer = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM)
        .setProvider(BouncyCastleProvider.PROVIDER_NAME)
        .build(caKey);

    return new JcaX509CertificateConverter()
        .setProvider(BouncyCastleProvider.PROVIDER_NAME)
        .getCertificate(certBuilder.build(signer));
  }

  private static void writePEM(Path path, String type, byte[] content) throws Exception
  {
    try (Writer fileWriter = new OutputStreamWriter(Files.newOutputStream(path), StandardCharsets.UTF_8);
         PemWriter pemWriter = new PemWriter(fileWriter)) {
      pemWriter.writeObject(new PemObject(type, content));
    }
  }

  private static Path createTrustStore(Path directory, X509Certificate caCert) throws Exception
  {
    Path trustStorePath = directory.resolve("truststore.p12");
    KeyStore trustStore = KeyStore.getInstance("PKCS12");
    trustStore.load(null, null);
    trustStore.setCertificateEntry("ca", caCert);

    try (FileOutputStream fos = new FileOutputStream(trustStorePath.toFile())) {
      trustStore.store(fos, STORE_PASSWORD.toCharArray());
    }

    return trustStorePath;
  }

  private static Path createKeyStore(
      Path directory,
      X509Certificate cert,
      PrivateKey key
  ) throws Exception
  {
    Path keyStorePath = directory.resolve("client.p12");
    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    keyStore.load(null, null);
    keyStore.setKeyEntry(
        "client",
        key,
        STORE_PASSWORD.toCharArray(),
        new Certificate[]{cert}
    );

    try (FileOutputStream fos = new FileOutputStream(keyStorePath.toFile())) {
      keyStore.store(fos, STORE_PASSWORD.toCharArray());
    }

    return keyStorePath;
  }
}
