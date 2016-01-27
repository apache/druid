/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.inject.name.Named;
import com.metamx.common.StringUtils;

import javax.validation.constraints.NotNull;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Namespaced extraction is a special case of DimExtractionFn where the actual extractor is pulled from a map of known implementations.
 * In the event that an unknown namespace is passed, a simple reflective function is returned instead.
 */
@JsonTypeName("namespace")
public class NamespacedExtractor extends LookupExtractor
{
  private static final byte CACHE_TYPE_ID = 0x05;

  private final String namespace;
  private final Function<String, String> extractionFunction;
  private final Function<String, List<String>> reverseExtractionFunction;

  @JsonCreator
  public NamespacedExtractor(
      @NotNull @JacksonInject @Named("dimExtractionNamespace")
      final Function<String, Function<String, String>> namespaces,
      @NotNull @JacksonInject @Named("dimReverseExtractionNamespace")
      final Function<String, Function<String, List<String>>> reverseNamespaces,
      @NotNull @JsonProperty(value = "namespace", required = true)
      final String namespace
  )
  {
    this.namespace = Preconditions.checkNotNull(namespace, "namespace");
    this.extractionFunction = Preconditions.checkNotNull(namespaces.apply(namespace), "no namespace found");
    this.reverseExtractionFunction = Preconditions.checkNotNull(
        reverseNamespaces.apply(namespace),
        "can not found reverse extraction function"
    );
  }

  @JsonProperty("namespace")
  public String getNamespace()
  {
    return namespace;
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] nsBytes = StringUtils.toUtf8(namespace);
    return ByteBuffer.allocate(nsBytes.length + 1).put(CACHE_TYPE_ID).put(nsBytes).array();
  }

  @Override
  public String apply(String value)
  {
    return extractionFunction.apply(value);
  }

  @Override
  public List<String> unapply(@NotNull String value)
  {
    return reverseExtractionFunction.apply(value);
  }
}
