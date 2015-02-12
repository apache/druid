/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sun.net.www.protocol.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.metamx.common.IAE;
import io.druid.common.aws.AWSCredentialsConfig;
import io.druid.common.aws.AWSCredentialsUtils;
import io.druid.storage.s3.AWSCredentialsGuiceBridge;
import io.druid.storage.s3.S3StorageDruidModule;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;

/**
 *
 */
public class HandlerTest
{

  private static class AWSCredentialsGuiceBridgeProvider implements Provider<AWSCredentialsGuiceBridge>{
    public AWSCredentialsGuiceBridge get(){
      AWSCredentialsGuiceBridge bridge = new AWSCredentialsGuiceBridge();
      bridge.setCredentials(
          new AWSCredentialsProviderChain(
              new AWSCredentialsProvider()
              {
                @Override
                public AWSCredentials getCredentials()
                {
                  return new AWSCredentials()
                  {
                    @Override
                    public String getAWSAccessKeyId()
                    {
                      return "abc";
                    }

                    @Override
                    public String getAWSSecretKey()
                    {
                      return "def";
                    }
                  };
                }

                @Override
                public void refresh()
                {
                  // NOOP
                }
              }
          )
      );
      return bridge;
    }
  }
  private static final String prefix = "s3";
  private static final String goodURL = prefix + "://elasticmapreduce/images/integrating_data_predicate_1.png";
  @BeforeClass
  public static void setupStatic(){
    Injector injector = Guice.createInjector(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bind(AWSCredentialsGuiceBridge.class).toProvider(AWSCredentialsGuiceBridgeProvider.class).asEagerSingleton();
          }
        }
    );
  }

  @Test(expected = AmazonS3Exception.class)
  // Test will fail due to bad credentials
  public void testGeneralURL() throws IOException
  {
    final URL url = new URL(goodURL);
    File file = File.createTempFile("handlerTest", ".png");
    file.deleteOnExit();
    try {
      if (!file.delete()) {
        throw new IOException(String.format("Unable to delete file [%s]", file.getAbsolutePath()));
      }
      try (InputStream stream = url.openStream()) {
        Files.copy(stream, file.toPath());
      }catch(AmazonS3Exception ex){
        Assert.assertTrue(ex.getMessage().startsWith("The AWS Access Key Id you provided does not exist in our records"));
        throw ex;
      }
    }
    finally {
      file.delete();
    }
  }

  @Test(expected = IAE.class)
  public void testBadSecret() throws IOException
  {
    final URL url = new URL(prefix + "://user:@bucket/key");
    try(InputStream stream = url.openStream()){

    }
  }

  @Test(expected = IAE.class)
  public void testBadUser() throws IOException
  {
    final URL url = new URL(prefix + "://:secret@bucket/key");
    try(InputStream stream = url.openStream()){

    }
  }

  @Test(expected = IAE.class)
  public void testBadUserInfo() throws IOException
  {
    final URL url = new URL(prefix + "://whoops@bucket/key");
    try(InputStream stream = url.openStream()){

    }
  }

  @Test(expected = IAE.class)
  public void testBadBucket() throws IOException
  {
    final URL url = new URL(prefix + ":///key");
    try(InputStream stream = url.openStream()){

    }
  }
  @Test(expected = IAE.class)
  public void testBadKey() throws IOException
  {
    final URL url = new URL(prefix + "://bucket/");
    try(InputStream stream = url.openStream()){

    }
  }

  @Test(expected = IAE.class)
  public void testBadBucketKey() throws IOException
  {
    final URL url = new URL(prefix + "://bucket");
    try(InputStream stream = url.openStream()){

    }
  }
}
