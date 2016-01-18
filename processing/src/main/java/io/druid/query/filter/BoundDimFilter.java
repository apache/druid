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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.metamx.common.StringUtils;

import java.nio.ByteBuffer;

public class BoundDimFilter implements DimFilter
{
  private final String dimension;
  private final String upper;
  private final String lower;
  private final boolean lowerStrict;
  private final boolean upperStrict;
  private final boolean alphaNumeric;

  @JsonCreator
  public BoundDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("lower") String lower,
      @JsonProperty("upper") String upper,
      @JsonProperty("lowerStrict") Boolean lowerStrict,
      @JsonProperty("upperStrict") Boolean upperStrict,
      @JsonProperty("alphaNumeric") Boolean alphaNumeric
  )
  {
    this.dimension = Preconditions.checkNotNull(dimension, "dimension can not be null");
    Preconditions.checkState((lower != null) || (upper != null), "lower and upper can not be null at the same time");
    this.upper = upper;
    this.lower = lower;
    this.lowerStrict = (lowerStrict == null) ? false : lowerStrict;
    this.upperStrict = (upperStrict == null) ? false : upperStrict;
    this.alphaNumeric = (alphaNumeric == null) ? false : alphaNumeric;
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getUpper()
  {
    return upper;
  }

  @JsonProperty
  public String getLower()
  {
    return lower;
  }

  @JsonProperty
  public boolean isLowerStrict()
  {
    return lowerStrict;
  }

  @JsonProperty
  public boolean isUpperStrict()
  {
    return upperStrict;
  }

  @JsonProperty
  public boolean isAlphaNumeric()
  {
    return alphaNumeric;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] dimensionBytes = StringUtils.toUtf8(this.getDimension());
    byte[] lowerBytes = this.getLower() == null ? new byte[0] : StringUtils.toUtf8(this.getLower());
    byte[] upperBytes = this.getUpper() == null ? new byte[0] : StringUtils.toUtf8(this.getUpper());
    byte boundType = 0x1;
    if (this.getLower() == null) {
      boundType = 0x2;
    } else if (this.getUpper() == null) {
      boundType = 0x3;
    }

    byte lowerStrictByte = (this.isLowerStrict() == false) ? 0x0 : (byte) 1;
    byte upperStrictByte = (this.isUpperStrict() == false) ? 0x0 : (byte) 1;
    byte AlphaNumericByte = (this.isAlphaNumeric() == false) ? 0x0 : (byte) 1;

    ByteBuffer boundCacheBuffer = ByteBuffer.allocate(
        8
        + dimensionBytes.length
        + upperBytes.length
        + lowerBytes.length
    );
    boundCacheBuffer.put(DimFilterCacheHelper.BOUND_CACHE_ID)
                    .put(boundType)
                    .put(upperStrictByte)
                    .put(lowerStrictByte)
                    .put(AlphaNumericByte)
                    .put(DimFilterCacheHelper.STRING_SEPARATOR)
                    .put(dimensionBytes)
                    .put(DimFilterCacheHelper.STRING_SEPARATOR)
                    .put(upperBytes)
                    .put(DimFilterCacheHelper.STRING_SEPARATOR)
                    .put(lowerBytes);
    return boundCacheBuffer.array();
  }

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BoundDimFilter)) {
      return false;
    }

    BoundDimFilter that = (BoundDimFilter) o;

    if (isLowerStrict() != that.isLowerStrict()) {
      return false;
    }
    if (isUpperStrict() != that.isUpperStrict()) {
      return false;
    }
    if (isAlphaNumeric() != that.isAlphaNumeric()) {
      return false;
    }
    if (!getDimension().equals(that.getDimension())) {
      return false;
    }
    if (getUpper() != null ? !getUpper().equals(that.getUpper()) : that.getUpper() != null) {
      return false;
    }
    return !(getLower() != null ? !getLower().equals(that.getLower()) : that.getLower() != null);

  }

  @Override
  public int hashCode()
  {
    int result = getDimension().hashCode();
    result = 31 * result + (getUpper() != null ? getUpper().hashCode() : 0);
    result = 31 * result + (getLower() != null ? getLower().hashCode() : 0);
    result = 31 * result + (isLowerStrict() ? 1 : 0);
    result = 31 * result + (isUpperStrict() ? 1 : 0);
    result = 31 * result + (isAlphaNumeric() ? 1 : 0);
    return result;
  }
}
