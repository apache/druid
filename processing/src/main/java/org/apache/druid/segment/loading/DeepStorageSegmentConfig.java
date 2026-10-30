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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.utils.CompressionUtils;

import javax.annotation.Nullable;

/**
 * The {@code druid.storage} properties that describe how segments are laid out in deep storage rather than how to
 * reach it, and so are shared by every {@link DataSegmentPusher} instead of belonging to any one of them. Currently
 * that is {@code druid.storage.compressionFormat} and deprecated {@code druid.storage.zip}, but anything else that is
 * a property of the layout rather than of a particular deep storage belongs here too.
 * <p>
 * This is bound once in core, by {@code LocalDataStorageDruidModule}, instead of once per deep storage
 * implementation, so that these properties mean the same thing whatever {@code druid.storage.type} is set to, and so
 * that each implementation does not have to redeclare them in its own config class. Any single-implementation
 * property still belongs in that implementation's own config, next to its bucket and credentials.
 * <p>
 * Values here are deliberately tri-state: when a property is unset, the getter falls back to the default the calling
 * implementation passes in, since those defaults differ per implementation.
 */
public class DeepStorageSegmentConfig
{
  @JsonProperty
  @Deprecated
  @Nullable
  private final Boolean zip;

  @JsonProperty
  @Nullable
  private final CompressionUtils.Format compressionFormat;

  @JsonCreator
  public DeepStorageSegmentConfig(
      @JsonProperty("zip") @Nullable Boolean zip,
      @JsonProperty("compressionFormat") @Nullable CompressionUtils.Format compressionFormat
  )
  {
    validate(zip, compressionFormat);
    this.zip = zip;
    this.compressionFormat = compressionFormat;
  }

  public DeepStorageSegmentConfig(@Nullable Boolean zip)
  {
    this(zip, null);
  }

  public DeepStorageSegmentConfig()
  {
    this(null, null);
  }

  private static void validate(@Nullable Boolean zip, @Nullable CompressionUtils.Format compressionFormat)
  {
    if (zip == null || compressionFormat == null) {
      return;
    }
    if (compressionFormat == CompressionUtils.Format.ZIP && !zip) {
      throw new IAE(
          "Conflicting compression configuration: druid.storage.compressionFormat is [%s] but druid.storage.zip is [%s]",
          compressionFormat,
          zip
      );
    }
    if (compressionFormat == CompressionUtils.Format.NONE && zip) {
      throw new IAE(
          "Conflicting compression configuration: druid.storage.compressionFormat is [%s] but druid.storage.zip is [%s]",
          compressionFormat,
          zip
      );
    }
    if (compressionFormat != CompressionUtils.Format.ZIP && compressionFormat != CompressionUtils.Format.NONE) {
      throw new IAE(
          "Conflicting compression configuration: druid.storage.zip cannot be configured when druid.storage.compressionFormat is [%s]",
          compressionFormat
      );
    }
  }

  /**
   * Resolves the effective compression format to use, falling back to {@code defaultFormat} when neither
   * {@code druid.storage.compressionFormat} nor {@code druid.storage.zip} has been set.
   */
  public CompressionUtils.Format getEffectiveCompressionFormat(CompressionUtils.Format defaultFormat)
  {
    Preconditions.checkNotNull(defaultFormat, "defaultFormat");
    if (compressionFormat != null) {
      return compressionFormat;
    }
    if (zip != null) {
      return zip ? CompressionUtils.Format.ZIP : CompressionUtils.Format.NONE;
    }
    return defaultFormat;
  }

  /**
   * Whether segments should be written as a zip file, falling back to {@code defaultZip} when
   * {@code druid.storage.zip} or {@code druid.storage.compressionFormat} has not been set.
   *
   * @deprecated Use {@link #getEffectiveCompressionFormat(CompressionUtils.Format)} instead.
   */
  @Deprecated
  public boolean isZip(boolean defaultZip)
  {
    final CompressionUtils.Format effective = getEffectiveCompressionFormat(
        defaultZip ? CompressionUtils.Format.ZIP : CompressionUtils.Format.NONE
    );
    return effective == CompressionUtils.Format.ZIP;
  }

  @JsonProperty("zip")
  @Deprecated
  @Nullable
  public Boolean getZip()
  {
    return zip;
  }

  @JsonProperty("compressionFormat")
  @Nullable
  public CompressionUtils.Format getCompressionFormat()
  {
    return compressionFormat;
  }
}
