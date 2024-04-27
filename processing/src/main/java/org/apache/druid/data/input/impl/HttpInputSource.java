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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.systemfield.SystemField;
import org.apache.druid.data.input.impl.systemfield.SystemFieldDecoratorFactory;
import org.apache.druid.data.input.impl.systemfield.SystemFieldInputSource;
import org.apache.druid.data.input.impl.systemfield.SystemFields;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.PasswordProvider;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

public class HttpInputSource
    extends AbstractInputSource
    implements SplittableInputSource<URI>, SystemFieldInputSource
{
  public static final String TYPE_KEY = "http";

  private final List<URI> uris;
  @Nullable
  private final String httpAuthenticationUsername;
  @Nullable
  private final PasswordProvider httpAuthenticationPasswordProvider;
  private final SystemFields systemFields;
  private final HttpInputSourceConfig config;

  @JsonCreator
  public HttpInputSource(
      @JsonProperty("uris") List<URI> uris,
      @JsonProperty("httpAuthenticationUsername") @Nullable String httpAuthenticationUsername,
      @JsonProperty("httpAuthenticationPassword") @Nullable PasswordProvider httpAuthenticationPasswordProvider,
      @JsonProperty(SYSTEM_FIELDS_PROPERTY) @Nullable SystemFields systemFields,
      @JacksonInject HttpInputSourceConfig config
  )
  {
    Preconditions.checkArgument(uris != null && !uris.isEmpty(), "Empty URIs");
    throwIfInvalidProtocols(config, uris);
    this.uris = uris;
    this.httpAuthenticationUsername = httpAuthenticationUsername;
    this.httpAuthenticationPasswordProvider = httpAuthenticationPasswordProvider;
    this.systemFields = systemFields == null ? SystemFields.none() : systemFields;
    this.config = config;
  }

  @JsonIgnore
  @Nonnull
  @Override
  public Set<String> getTypes()
  {
    return Collections.singleton(TYPE_KEY);
  }

  public static void throwIfInvalidProtocols(HttpInputSourceConfig config, List<URI> uris)
  {
    for (URI uri : uris) {
      if (!config.getAllowedProtocols().contains(StringUtils.toLowerCase(uri.getScheme()))) {
        throw new IAE("Only %s protocols are allowed", config.getAllowedProtocols());
      }
    }
  }

  @JsonProperty
  public List<URI> getUris()
  {
    return uris;
  }

  @Override
  public Set<SystemField> getConfiguredSystemFields()
  {
    return systemFields.getFields();
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getHttpAuthenticationUsername()
  {
    return httpAuthenticationUsername;
  }

  @Nullable
  @JsonProperty("httpAuthenticationPassword")
  @JsonInclude(JsonInclude.Include.NON_NULL)
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
        httpAuthenticationPasswordProvider,
        systemFields,
        config
    );
  }

  @Override
  public Object getSystemFieldValue(InputEntity entity, SystemField field)
  {
    final HttpEntity httpEntity = (HttpEntity) entity;

    switch (field) {
      case URI:
        return httpEntity.getUri().toString();
      case PATH:
        return httpEntity.getPath();
      default:
        return null;
    }
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
        CloseableIterators.withEmptyBaggage(
            createSplits(inputFormat, null).map(split -> new HttpEntity(
                split.get(),
                httpAuthenticationUsername,
                httpAuthenticationPasswordProvider
            )).iterator()
        ),
        SystemFieldDecoratorFactory.fromInputSource(this),
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
    HttpInputSource that = (HttpInputSource) o;
    return Objects.equals(uris, that.uris)
           && Objects.equals(httpAuthenticationUsername, that.httpAuthenticationUsername)
           && Objects.equals(httpAuthenticationPasswordProvider, that.httpAuthenticationPasswordProvider)
           && Objects.equals(systemFields, that.systemFields)
           && Objects.equals(config, that.config);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(uris, httpAuthenticationUsername, httpAuthenticationPasswordProvider, systemFields, config);
  }

  @Override
  public boolean needsFormat()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "HttpInputSource{" +
           "uris=\"" + uris + "\"" +
           ", httpAuthenticationUsername=" + httpAuthenticationUsername +
           ", httpAuthenticationPasswordProvider=" + httpAuthenticationPasswordProvider +
           (systemFields.getFields().isEmpty() ? "" : ", systemFields=" + systemFields) +
           "}";
  }
}
