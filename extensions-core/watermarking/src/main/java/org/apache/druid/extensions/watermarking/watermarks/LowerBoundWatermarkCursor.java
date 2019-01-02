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

package org.apache.druid.extensions.watermarking.watermarks;

import com.google.common.base.Strings;
import org.apache.druid.extensions.watermarking.storage.WatermarkSink;
import org.apache.druid.extensions.watermarking.storage.cache.WatermarkCache;
import org.joda.time.DateTime;
import org.joda.time.Interval;

public abstract class LowerBoundWatermarkCursor extends WatermarkCursor
{
  private String lowerBoundWatermarkType;
  private DateTime lowerBoundExisting;

  public LowerBoundWatermarkCursor(
      WatermarkCache watermarkCache,
      WatermarkSink watermarkSink,
      String dataSource,
      Interval interval,
      String watermarkType
  )
  {
    super(watermarkCache, watermarkSink, dataSource, interval);
    this.lowerBoundWatermarkType = watermarkType;
    setLowerBoundExisting();
  }

  @Override
  public void reset()
  {
    super.reset();
    if (!Strings.isNullOrEmpty(lowerBoundWatermarkType)) {
      setLowerBoundExisting();
    }
  }

  @Override
  public DateTime getMetadata()
  {
    DateTime candidate = super.getMetadata();
    return lowerBoundExisting == null ||
           (candidate != null && candidate.isAfter(lowerBoundExisting)) ? candidate : lowerBoundExisting;

  }

  @Override
  public DateTime getWatermark()
  {
    DateTime candidate = super.getWatermark();
    return lowerBoundExisting == null ||
           (candidate != null && candidate.isAfter(lowerBoundExisting)) ? candidate : lowerBoundExisting;

  }

  private void setLowerBoundExisting()
  {
    lowerBoundExisting = getWatermarkCache().get(getDataSource(), lowerBoundWatermarkType);
  }
}
