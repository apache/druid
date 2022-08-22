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

package org.apache.druid.server.initialization;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import org.apache.commons.io.IOUtils;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.HttpClientConfig;
import org.apache.druid.java.util.http.client.HttpClientInit;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.InputStreamResponseHandler;
import org.apache.druid.metadata.PasswordProvider;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.apache.druid.server.initialization.jetty.ServletFilterHolder;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.eclipse.jetty.server.Server;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.ws.rs.core.MediaType;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.text.SimpleDateFormat;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.GZIPOutputStream;

public class JettyCertRenewTest extends BaseJettyTest
{
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private Injector injector;

  private LatchedRequestStateHolder latchedRequestState;

  private Path tmpKeyStore;

  private Path tmpTrustStore;

  private PasswordProvider pp;

  @Override
  public void setProperties()
  {
    // call super.setProperties first in case it is setting the same property as this class
    super.setProperties();
    System.setProperty("druid.server.http.showDetailedJettyErrors", "true");
  }


  @Override
  protected Injector setupInjector()
  {
    TLSServerConfig tlsConfig;
    try {
      File keyStore = new File(JettyCertRenewTest.class.getClassLoader().getResource("server.jks").getFile());
      tmpKeyStore = Files.copy(keyStore.toPath(), new File(folder.newFolder(), "server.jks").toPath());
      File trustStore = new File(JettyCertRenewTest.class.getClassLoader().getResource("truststore.jks").getFile());
      tmpTrustStore = Files.copy(trustStore.toPath(), new File(folder.newFolder(), "truststore.jks").toPath());
      pp = () -> "druid123";
      tlsConfig = new TLSServerConfig()
      {
        @Override
        public String getKeyStorePath()
        {
          return tmpKeyStore.toString();
        }

        @Override
        public String getKeyStoreType()
        {
          return "jks";
        }

        @Override
        public PasswordProvider getKeyStorePasswordProvider()
        {
          return pp;
        }

        @Override
        public PasswordProvider getKeyManagerPasswordProvider()
        {
          return pp;
        }

        @Override
        public String getTrustStorePath()
        {
          return tmpTrustStore.toString();
        }

        @Override
        public String getTrustStoreAlgorithm()
        {
          return "PKIX";
        }

        @Override
        public PasswordProvider getTrustStorePasswordProvider()
        {
          return pp;
        }

        @Override
        public String getCertAlias()
        {
          return "druid";
        }

        @Override
        public boolean isRequireClientCertificate()
        {
          return false;
        }

        @Override
        public boolean isRequestClientCertificate()
        {
          return false;
        }

        @Override
        public boolean isValidateHostnames()
        {
          return false;
        }

        @Override
        public boolean isReloadSslContext()
        {
          return true;
        }

        @Override
        public int getReloadSslContextSeconds()
        {
          return 1;
        }
      };
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    final int ephemeralPort = ThreadLocalRandom.current().nextInt(49152, 65535);

    latchedRequestState = new LatchedRequestStateHolder();
    injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Module>of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder,
                    Key.get(DruidNode.class, Self.class),
                    new DruidNode("test", "localhost", false, ephemeralPort, ephemeralPort + 1, true, true)
                );
                binder.bind(TLSServerConfig.class).toInstance(tlsConfig);
                binder.bind(JettyServerInitializer.class).to(JettyServerInit.class).in(LazySingleton.class);
                binder.bind(LatchedRequestStateHolder.class).toInstance(latchedRequestState);

                Multibinder<ServletFilterHolder> multibinder = Multibinder.newSetBinder(
                    binder,
                    ServletFilterHolder.class
                );

                multibinder.addBinding().toInstance(
                    new ServletFilterHolder()
                    {
                      @Override
                      public String getPath()
                      {
                        return "/*";
                      }

                      @Override
                      public Map<String, String> getInitParameters()
                      {
                        return null;
                      }

                      @Override
                      public Class<? extends Filter> getFilterClass()
                      {
                        return DummyAuthFilter.class;
                      }

                      @Override
                      public Filter getFilter()
                      {
                        return null;
                      }

                      @Override
                      public EnumSet<DispatcherType> getDispatcherType()
                      {
                        return null;
                      }
                    }
                );


                Jerseys.addResource(binder, SlowResource.class);
                Jerseys.addResource(binder, LatchedResource.class);
                Jerseys.addResource(binder, ExceptionResource.class);
                Jerseys.addResource(binder, DefaultResource.class);
                Jerseys.addResource(binder, DirectlyReturnResource.class);
                binder.bind(AuthorizerMapper.class).toInstance(AuthTestUtils.TEST_AUTHORIZER_MAPPER);
                LifecycleModule.register(binder, Server.class);
              }
            }
        )
    );

    return injector;
  }


  @Test
  public void testCertificateEndDateInvalid() throws Exception
  {
    SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH);

    Certificate[] certificatesBefore = getCertificates();
    for (Certificate certificate : certificatesBefore) {
      X509Certificate real = (X509Certificate) certificate;
      Assert.assertEquals(dateFormat.parse("Fri Mar 29 11:00:40 UTC 2030").toInstant(), real.getNotAfter().toInstant());
    }

    Assert.assertEquals(DEFAULT_RESPONSE_CONTENT, getResponseWithProperTrustStore());

    // Replace the server and trustore keystores, wait for 3s and perform all the tests.
    File keyStore = new File(JettyCertRenewTest.class.getClassLoader().getResource("server-new.jks").getFile());
    Files.copy(keyStore.toPath(), tmpKeyStore, StandardCopyOption.REPLACE_EXISTING);
    File trustStore = new File(JettyCertRenewTest.class.getClassLoader().getResource("truststore-new.jks").getFile());
    Files.copy(trustStore.toPath(), tmpTrustStore, StandardCopyOption.REPLACE_EXISTING);

    Thread.sleep(3000);

    Certificate[] certificatesAfter = getCertificates();
    for (Certificate certificate : certificatesAfter) {
      X509Certificate real = (X509Certificate) certificate;
      Assert.assertEquals(dateFormat.parse("Thu Aug 19 13:38:51 UTC 2032").toInstant(), real.getNotAfter().toInstant());
    }

    Assert.assertEquals(DEFAULT_RESPONSE_CONTENT, getResponseWithProperTrustStore());
  }

  private static class AcceptAllForTestX509TrustManager implements X509TrustManager
  {
    private X509Certificate[] accepted;

    @Override
    public void checkClientTrusted(X509Certificate[] xcs, String string)
    {
    }

    @Override
    public void checkServerTrusted(X509Certificate[] xcs, String string)
    {
      accepted = xcs;
    }

    @Override
    public X509Certificate[] getAcceptedIssuers()
    {
      return accepted;
    }
  }

  private static class AcceptAllForTestHostnameVerifier implements HostnameVerifier
  {
    @Override
    public boolean verify(String string, SSLSession ssls)
    {
      return true;
    }
  }

  private Certificate[] getCertificates() throws Exception
  {
    URL url = new URL("https://localhost:" + tlsPort + "/default/");

    SSLContext sslCtx = SSLContext.getInstance("TLS");
    sslCtx.init(null, new TrustManager[]{new AcceptAllForTestX509TrustManager()}, null);

    HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();

    connection.setHostnameVerifier(new AcceptAllForTestHostnameVerifier());
    connection.setSSLSocketFactory(sslCtx.getSocketFactory());

    connection.getResponseCode();

    Certificate[] certificates = connection.getServerCertificates();

    connection.disconnect();

    return certificates;
  }

  private HttpClientConfig getSslConfig()
  {
    return HttpClientConfig.builder()
                           .withSslContext(
                               HttpClientInit.sslContextWithTrustedKeyStore(tmpTrustStore.toString(), pp.getPassword())
                           )
                           .withWorkerCount(1)
                           .withReadTimeout(Duration.ZERO)
                           .build();
  }

  private String getResponseWithProperTrustStore() throws Exception
  {
    String text = "hello";
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(out)) {
      gzipOutputStream.write(text.getBytes(Charset.defaultCharset()));
    }
    Request request = new Request(HttpMethod.GET, new URL("https://localhost:" + tlsPort + "/default/"));
    request.setHeader("Content-Encoding", "gzip");
    request.setContent(MediaType.TEXT_PLAIN, out.toByteArray());

    HttpClient client;
    try {
      client = HttpClientInit.createClient(
          getSslConfig(),
          lifecycle
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    ListenableFuture<InputStream> go = client.go(
        request,
        new InputStreamResponseHandler()
    );
    return IOUtils.toString(go.get(), StandardCharsets.UTF_8);
  }
}
