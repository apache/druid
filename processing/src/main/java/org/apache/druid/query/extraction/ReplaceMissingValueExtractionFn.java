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

package org.apache.druid.query.extraction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Functions;
import org.apache.druid.java.util.common.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ReplaceMissingValueExtractionFn extends FunctionalExtraction
{

  /**
   * ReplaceMissingValueExtractionFn is a coalesce-like extraction function: nulls will be replaced with the given value.
   *
   * @param replaceMissingValueWith String value to replace missing values with (instead of `null`)
   */
  @JsonCreator
  public ReplaceMissingValueExtractionFn(
          @JsonProperty("replaceMissingValueWith") final String replaceMissingValueWith
  )
  {
    super(Functions.identity(), false, replaceMissingValueWith, false);
  }

  @Override
  public byte[] getCacheKey()
  {
    try {
      final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      outputStream.write(ExtractionCacheHelper.CACHE_TYPE_ID_MISSINGVALUE);
      if (getReplaceMissingValueWith() != null) {
        outputStream.write(StringUtils.toUtf8(getReplaceMissingValueWith()));
      }

      return outputStream.toByteArray();
    }
    catch (IOException ex) {
      // If ByteArrayOutputStream.write has problems, that is a very bad thing
      throw new RuntimeException(ex);
    }
  }
}
