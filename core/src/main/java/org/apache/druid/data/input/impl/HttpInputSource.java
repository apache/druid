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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.metadata.PasswordProvider;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class HttpInputSource extends AbstractInputSource implements SplittableInputSource<URI>
{
  private final List<URI> uris;
  @Nullable
  private final String httpAuthenticationUsername;
  @Nullable
  private final PasswordProvider httpAuthenticationPasswordProvider;

  @JsonCreator
  public HttpInputSource(
      @JsonProperty("uris") List<URI> uris,
      @JsonProperty("httpAuthenticationUsername") @Nullable String httpAuthenticationUsername,
      @JsonProperty("httpAuthenticationPassword") @Nullable PasswordProvider httpAuthenticationPasswordProvider
  )
  {
    Preconditions.checkArgument(uris != null && !uris.isEmpty(), "Empty URIs");
    this.uris = uris;
    this.httpAuthenticationUsername = httpAuthenticationUsername;
    this.httpAuthenticationPasswordProvider = httpAuthenticationPasswordProvider;
  }

  @JsonProperty
  public List<URI> getUris()
  {
    return uris;
  }

  @Nullable
  @JsonProperty
  public String getHttpAuthenticationUsername()
  {
    return httpAuthenticationUsername;
  }

  @Nullable
  @JsonProperty("httpAuthenticationPassword")
  public PasswordProvider getHttpAuthenticationPasswordProvider()
  {
    return httpAuthenticationPasswordProvider;
  }

  @Override
  public Stream<InputSplit<URI>> createSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    return uris.stream().map(InputSplit::new);
  }

  @Override
  public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    return uris.size();
  }

  @Override
  public SplittableInputSource<URI> withSplit(InputSplit<URI> split)
  {
    return new HttpInputSource(
        Collections.singletonList(split.get()),
        httpAuthenticationUsername,
        httpAuthenticationPasswordProvider
    );
  }

  @Override
  protected InputSourceReader formattableReader(
      InputRowSchema inputRowSchema,
      InputFormat inputFormat,
      @Nullable File temporaryDirectory
  )
  {
    return new InputEntityIteratingReader(
        inputRowSchema,
        inputFormat,
        createSplits(inputFormat, null).map(split -> new HttpEntity(
            split.get(),
            httpAuthenticationUsername,
            httpAuthenticationPasswordProvider
        )).iterator(),
        temporaryDirectory
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HttpInputSource source = (HttpInputSource) o;
    return Objects.equals(uris, source.uris) &&
           Objects.equals(httpAuthenticationUsername, source.httpAuthenticationUsername) &&
           Objects.equals(httpAuthenticationPasswordProvider, source.httpAuthenticationPasswordProvider);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(uris, httpAuthenticationUsername, httpAuthenticationPasswordProvider);
  }

  @Override
  public boolean needsFormat()
  {
    return true;
  }
}
