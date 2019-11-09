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

package org.apache.druid.data.input.impl;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import org.apache.druid.data.input.ObjectSource;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.PasswordProvider;
import org.apache.druid.utils.CompressionUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLConnection;
import java.util.Base64;

public class HttpSource implements ObjectSource<URI>
{
  private final URI split;
  @Nullable
  private final String httpAuthenticationUsername;
  @Nullable
  private final PasswordProvider httpAuthenticationPasswordProvider;

  HttpSource(
      URI split,
      @Nullable String httpAuthenticationUsername,
      @Nullable PasswordProvider httpAuthenticationPasswordProvider
  )
  {
    this.split = split;
    this.httpAuthenticationUsername = httpAuthenticationUsername;
    this.httpAuthenticationPasswordProvider = httpAuthenticationPasswordProvider;
  }

  @Override
  public URI getObject()
  {
    return split;
  }

  @Override
  public InputStream open() throws IOException
  {
    return CompressionUtils.decompress(
        openURLConnection(split, httpAuthenticationUsername, httpAuthenticationPasswordProvider).getInputStream(),
        split.toString()
    );
  }

  @Override
  public Predicate<Throwable> getRetryCondition()
  {
    return t -> t instanceof IOException;
  }

  public static URLConnection openURLConnection(URI object, String userName, PasswordProvider passwordProvider)
      throws IOException
  {
    URLConnection urlConnection = object.toURL().openConnection();
    if (!Strings.isNullOrEmpty(userName) && passwordProvider != null) {
      String userPass = userName + ":" + passwordProvider.getPassword();
      String basicAuthString = "Basic " + Base64.getEncoder().encodeToString(StringUtils.toUtf8(userPass));
      urlConnection.setRequestProperty("Authorization", basicAuthString);
    }
    return urlConnection;
  }
}
