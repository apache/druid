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
import org.apache.druid.java.util.common.logger.Logger;
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
  private static final Logger log = new Logger(DeepStorageSegmentConfig.class);

  @JsonProperty("compressionFormat")
  @Nullable
  private final CompressionUtils.Format compressionFormat;

  @JsonCreator
  public DeepStorageSegmentConfig(
      @JsonProperty("zip") @Nullable Boolean zip,
      @JsonProperty("compressionFormat") @Nullable CompressionUtils.Format compressionFormat
  )
  {
    if (zip != null) {
      log.warn("druid.storage.zip is deprecated, use druid.storage.compressionFormat instead.");
    }
    if (compressionFormat != null) {
      this.compressionFormat = compressionFormat;
    } else if (zip != null) {
      this.compressionFormat = zip ? CompressionUtils.Format.ZIP : CompressionUtils.Format.NONE;
    } else {
      this.compressionFormat = null;
    }
  }

  public DeepStorageSegmentConfig()
  {
    this(null, null);
  }

  /**
   * Resolves the effective compression format to use, falling back to {@code defaultFormat} when neither
   * {@code druid.storage.compressionFormat} nor {@code druid.storage.zip} has been set.
   */
  public CompressionUtils.Format getEffectiveCompressionFormat(CompressionUtils.Format defaultFormat)
  {
    Preconditions.checkNotNull(defaultFormat, "defaultFormat");
    return compressionFormat != null ? compressionFormat : defaultFormat;
  }

  @JsonProperty("compressionFormat")
  @Nullable
  public CompressionUtils.Format getCompressionFormat()
  {
    return compressionFormat;
  }
}
