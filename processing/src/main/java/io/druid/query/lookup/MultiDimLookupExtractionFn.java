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

package io.druid.query.lookup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.metamx.common.StringUtils;
import io.druid.query.extraction.ExtractionCacheHelper;
import io.druid.query.extraction.MultiInputFunctionalExtraction;
import org.apache.commons.collections.keyvalue.MultiKey;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

public class MultiDimLookupExtractionFn extends MultiInputFunctionalExtraction
{
  private final LookupExtractor lookup;
  private final boolean optimize;
  private final String replaceMissingValueWith;
  private final Integer numKeys;

  @JsonCreator
  public MultiDimLookupExtractionFn(
      @JsonProperty("lookup") final LookupExtractor lookup,
      @Nullable @JsonProperty("replaceMissingValueWith") final String replaceMissingValueWith,
      @JsonProperty("optimize") Boolean optimize,
      @JsonProperty("numKeys") final Integer numKeys
  )
  {
    super(
        new Function<List<String>, String>()
        {
          @Nullable
          @Override
          public String apply(List<String> inputList)
          {
            Preconditions.checkArgument(inputList.size() == numKeys,
                String.format("Number of Keys should be %d", numKeys));
            String[] inputArray = new String[inputList.size()];
            inputArray = inputList.toArray(inputArray);
            MultiKey key = new MultiKey(inputArray);
            return lookup.apply(key);
          }
        },
        replaceMissingValueWith
    );
    this.lookup = lookup;
    this.optimize = optimize == null ?  true : optimize;
    this.replaceMissingValueWith = replaceMissingValueWith;
    Preconditions.checkArgument(numKeys > 1, "number of dimension keys should be greater than 1");
    this.numKeys = numKeys;
  }

  @JsonProperty
  public LookupExtractor getLookup()
  {
    return lookup;
  }

  @Override
  @JsonProperty
  public String getReplaceMissingValueWith()
  {
    return super.getReplaceMissingValueWith();
  }

  @JsonProperty("optimize")
  public boolean isOptimize()
  {
    return optimize;
  }

  @JsonProperty
  public Integer getNumKeys()
  {
    return numKeys;
  }

  @Override
  public byte[] getCacheKey() {
    try {
      final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      outputStream.write(ExtractionCacheHelper.CACHE_TYPE_ID_MULTILOOKUP);
      outputStream.write(lookup.getCacheKey());
      if (getReplaceMissingValueWith() != null) {
        outputStream.write(StringUtils.toUtf8(getReplaceMissingValueWith()));
        outputStream.write(ExtractionCacheHelper.CACHE_KEY_SEPARATOR);
      }
      outputStream.write(isOptimize() ? 1 : 0);
      outputStream.write(getNumKeys());
      return outputStream.toByteArray();
    }
    catch (IOException ex) {
      // If ByteArrayOutputStream.write has problems, that is a very bad thing
      throw Throwables.propagate(ex);
    }
  }

  @Override
  public int arity()
  {
    return numKeys;
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

    MultiDimLookupExtractionFn that = (MultiDimLookupExtractionFn) o;

    if (isOptimize() != that.isOptimize()) {
      return false;
    }
    if (!getNumKeys().equals(that.getNumKeys())) {
      return false;
    }
    if (getLookup() != null ? !getLookup().equals(that.getLookup()) : that.getLookup() != null) {
      return false;
    }
    return getReplaceMissingValueWith() != null
        ? getReplaceMissingValueWith().equals(that.getReplaceMissingValueWith())
        : that.getReplaceMissingValueWith() == null;

  }

  @Override
  public int hashCode()
  {
    int result = getLookup() != null ? getLookup().hashCode() : 0;
    result = 31 * result + (isOptimize() ? 1 : 0);
    result = 31 * result + (getReplaceMissingValueWith() != null ? getReplaceMissingValueWith().hashCode() : 0);
    result = 31 * result + getNumKeys().hashCode();
    return result;
  }
}
