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
import com.google.common.net.HttpHeaders;
import org.apache.druid.data.input.RetryingInputEntity;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.PasswordProvider;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLConnection;
import java.util.Base64;

public class HttpEntity extends RetryingInputEntity
{
  private static final Logger LOG = new Logger(HttpEntity.class);

  private final URI uri;
  @Nullable
  private final String httpAuthenticationUsername;
  @Nullable
  private final PasswordProvider httpAuthenticationPasswordProvider;

  HttpEntity(
      URI uri,
      @Nullable String httpAuthenticationUsername,
      @Nullable PasswordProvider httpAuthenticationPasswordProvider
  )
  {
    this.uri = uri;
    this.httpAuthenticationUsername = httpAuthenticationUsername;
    this.httpAuthenticationPasswordProvider = httpAuthenticationPasswordProvider;
  }

  @Override
  public URI getUri()
  {
    return uri;
  }

  @Override
  protected InputStream readFrom(long offset) throws IOException
  {
    return openInputStream(uri, httpAuthenticationUsername, httpAuthenticationPasswordProvider, offset);
  }

  @Override
  protected String getPath()
  {
    return uri.getPath();
  }

  @Override
  public Predicate<Throwable> getRetryCondition()
  {
    return t -> t instanceof IOException;
  }

  public static InputStream openInputStream(URI object, String userName, PasswordProvider passwordProvider, long offset)
      throws IOException
  {
    final URLConnection urlConnection = object.toURL().openConnection();
    if (!Strings.isNullOrEmpty(userName) && passwordProvider != null) {
      String userPass = userName + ":" + passwordProvider.getPassword();
      String basicAuthString = "Basic " + Base64.getEncoder().encodeToString(StringUtils.toUtf8(userPass));
      urlConnection.setRequestProperty("Authorization", basicAuthString);
    }
    final String acceptRanges = urlConnection.getHeaderField(HttpHeaders.ACCEPT_RANGES);
    final boolean withRanges = "bytes".equalsIgnoreCase(acceptRanges);
    if (withRanges && offset > 0) {
      // Set header for range request.
      // Since we need to set only the start offset, the header is "bytes=<range-start>-".
      // See https://tools.ietf.org/html/rfc7233#section-2.1
      urlConnection.addRequestProperty(HttpHeaders.RANGE, StringUtils.format("bytes=%d-", offset));
      return urlConnection.getInputStream();
    } else {
      if (!withRanges && offset > 0) {
        LOG.warn(
            "Since the input source doesn't support range requests, the object input stream is opened from the start and "
            + "then skipped. This may make the ingestion speed slower. Consider enabling prefetch if you see this message"
            + " a lot."
        );
      }
      final InputStream in = urlConnection.getInputStream();
      final long skipped = in.skip(offset);
      if (skipped != offset) {
        throw new ISE("Requested to skip [%s] bytes, but actual number of bytes skipped is [%s]", offset, skipped);
      }
      return in;
    }

  }
}
