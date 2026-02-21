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
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyUsage;
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
import java.nio.file.attribute.PosixFilePermissions;
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

  private static X500Name createX500Name(String commonName)
  {
    X500NameBuilder builder = new X500NameBuilder();
    builder.addRDN(org.bouncycastle.asn1.x500.style.RFC4519Style.c, "US");
    builder.addRDN(org.bouncycastle.asn1.x500.style.RFC4519Style.o, "Apache Druid Test");
    builder.addRDN(org.bouncycastle.asn1.x500.style.RFC4519Style.cn, commonName);
    return builder.build();
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
    // Set readable and executable permissions for all users (required for containers)
    tempDir.toFile().setReadable(true, false);
    tempDir.toFile().setExecutable(true, false);
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

    // Generate server certificate with SAN for localhost and additional hostnames
    KeyPair serverKeyPair = generateKeyPair();
    X509Certificate serverCert = generateServerCertificate(
        serverKeyPair,
        caKeyPair.getPrivate(),
        new String[]{"localhost", "127.0.0.1", "server.dc1", "consul"}
    );

    // Generate client certificate for mTLS
    KeyPair clientKeyPair = generateKeyPair();
    X509Certificate clientCert = generateClientCertificate(
        clientKeyPair,
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

  private static X509v3CertificateBuilder createCertificateBuilder(X500Name subject, KeyPair keyPair)
  {
    X500Name issuer = createX500Name("Test CA");
    BigInteger serial = BigInteger.valueOf(System.currentTimeMillis());
    Date notBefore = new Date();
    Date notAfter = new Date(notBefore.getTime() + VALIDITY_DAYS * 24 * 60 * 60 * 1000L);

    return new JcaX509v3CertificateBuilder(
        issuer,
        serial,
        notBefore,
        notAfter,
        subject,
        keyPair.getPublic()
    );
  }

  private static X509Certificate buildCertificate(X509v3CertificateBuilder certBuilder, PrivateKey signingKey) throws Exception
  {
    ContentSigner signer = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM)
        .setProvider(BouncyCastleProvider.PROVIDER_NAME)
        .build(signingKey);

    return new JcaX509CertificateConverter()
        .setProvider(BouncyCastleProvider.PROVIDER_NAME)
        .getCertificate(certBuilder.build(signer));
  }

  private static X509Certificate generateCACertificate(KeyPair keyPair) throws Exception
  {
    X500Name subject = createX500Name("Test CA");
    X509v3CertificateBuilder certBuilder = createCertificateBuilder(subject, keyPair);

    certBuilder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
    certBuilder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));

    return buildCertificate(certBuilder, keyPair.getPrivate());
  }

  private static boolean isIpAddress(String hostname)
  {
    if (hostname == null || hostname.isEmpty()) {
      return false;
    }
    String[] parts = hostname.split("\\.");
    if (parts.length != 4) {
      return false;
    }
    try {
      for (String part : parts) {
        int value = Integer.parseInt(part);
        if (value < 0 || value > 255) {
          return false;
        }
      }
      return true;
    }
    catch (NumberFormatException e) {
      return false;
    }
  }

  private static X509Certificate generateServerCertificate(
      KeyPair keyPair,
      PrivateKey caKey,
      String[] hostnames
  ) throws Exception
  {
    X500Name subject = createX500Name(hostnames[0]);
    X509v3CertificateBuilder certBuilder = createCertificateBuilder(subject, keyPair);

    GeneralName[] altNames = new GeneralName[hostnames.length];
    for (int i = 0; i < hostnames.length; i++) {
      String hostname = hostnames[i];
      if (isIpAddress(hostname)) {
        altNames[i] = new GeneralName(GeneralName.iPAddress, hostname);
      } else {
        altNames[i] = new GeneralName(GeneralName.dNSName, hostname);
      }
    }
    certBuilder.addExtension(Extension.subjectAlternativeName, false, new GeneralNames(altNames));
    certBuilder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));

    return buildCertificate(certBuilder, caKey);
  }

  private static X509Certificate generateClientCertificate(
      KeyPair keyPair,
      PrivateKey caKey,
      String commonName
  ) throws Exception
  {
    X500Name subject = createX500Name(commonName);
    X509v3CertificateBuilder certBuilder = createCertificateBuilder(subject, keyPair);

    certBuilder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));

    return buildCertificate(certBuilder, caKey);
  }

  private static void writePEM(Path path, String type, byte[] content) throws Exception
  {
    try (Writer fileWriter = new OutputStreamWriter(Files.newOutputStream(path), StandardCharsets.UTF_8);
         PemWriter pemWriter = new PemWriter(fileWriter)) {
      pemWriter.writeObject(new PemObject(type, content));
    }
    setWorldReadable(path);
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

    setWorldReadable(trustStorePath);

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

    setWorldReadable(keyStorePath);

    return keyStorePath;
  }

  private static void setWorldReadable(Path path)
  {
    try {
      Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-r--r--"));
    }
    catch (Exception e) {
      path.toFile().setReadable(true, false);
    }
  }
}
