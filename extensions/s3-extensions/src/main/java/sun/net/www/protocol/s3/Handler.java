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
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.metamx.common.IAE;
import io.druid.storage.s3.AWSCredentialsGuiceBridge;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.ServiceLoader;

/**
 *
 */
public class Handler extends URLStreamHandler
{
  @Override
  protected URLConnection openConnection(URL url) throws IOException
  {
    final String userInfo = url.getUserInfo();
    final AWSCredentialsProviderChain chain;
    if (!Strings.isNullOrEmpty(userInfo)) {
      final String user;
      final String pass;
      final String[] userPass = userInfo.split(":");
      if (userPass.length != 2) {
        throw new IAE("Malformed key/secret");
      }
      if (!Strings.isNullOrEmpty(userPass[0])) {
        user = userPass[0];
      } else {
        throw new IAE("Missing access key");
      }
      if (!Strings.isNullOrEmpty(userPass[1])) {
        pass = userPass[1];
      } else {
        throw new IAE("Missing access secret");
      }
      chain = new AWSCredentialsProviderChain(
          new StaticCredentialsProvider(
              new AWSCredentials()
              {
                @Override
                public String getAWSAccessKeyId()
                {
                  return user;
                }

                @Override
                public String getAWSSecretKey()
                {
                  return pass;
                }
              }
          ), new EnvironmentVariableCredentialsProvider(),
          new SystemPropertiesCredentialsProvider(),
          new ProfileCredentialsProvider(),
          new InstanceProfileCredentialsProvider()
      );
    } else {
      final ServiceLoader<AWSCredentialsGuiceBridge> bridge = ServiceLoader.load(
          AWSCredentialsGuiceBridge.class,
          getClass().getClassLoader()
      );
      chain = bridge.iterator().next().getCredentials();
    }

    final String host = url.getHost();
    if (Strings.isNullOrEmpty(host)) {
      throw new IAE("Must specify host/bucket");
    }
    final String[] hostParts = host.split("\\.");
    final String bucket = hostParts[0];

    final String path = url.getPath();
    if (Strings.isNullOrEmpty(path)) {
      throw new IAE("Must specify path/key");
    }

    if (path.length() < 2) {
      throw new IAE("Cannot have zero length key. Found [%s]", path);
    }
    final String key = path.substring(1); // eliminate `/`


    final URLConnection connection = new URLConnection(url)
    {
      AmazonS3Client amazonS3Client;

      @Override
      public void connect() throws IOException
      {
        amazonS3Client = new AmazonS3Client(chain);
      }

      @Override
      public InputStream getInputStream()
      {
        final GetObjectRequest request = new GetObjectRequest(bucket, key);
        final S3Object s3Object = amazonS3Client.getObject(request);
        return s3Object.getObjectContent();
      }
    };
    connection.connect();
    return connection;
  }
}
