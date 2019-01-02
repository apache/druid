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

import org.apache.druid.extensions.timeline.metadata.TimelineMetadataCursor;
import org.apache.druid.extensions.timeline.metadata.TimelineSegment;
import org.apache.druid.extensions.watermarking.storage.WatermarkSink;
import org.apache.druid.extensions.watermarking.storage.cache.WatermarkCache;
import org.apache.druid.java.util.common.logger.Logger;
import org.joda.time.DateTime;
import org.joda.time.Interval;

public abstract class WatermarkCursor implements Watermark, TimelineMetadataCursor<DateTime>
{
  private static final Logger log = new Logger(WatermarkCursor.class);
  protected final WatermarkCache watermarkCache;
  protected final WatermarkSink watermarkSink;
  protected final Interval interval;
  protected final String dataSource;
  protected DateTime watermark;
  protected DateTime cursorTimestamp;


  public WatermarkCursor(
      WatermarkCache watermarkCache,
      WatermarkSink watermarkSink,
      String dataSource,
      Interval interval
  )
  {
    this.dataSource = dataSource;
    this.interval = interval;
    this.watermarkCache = watermarkCache;
    this.watermarkSink = watermarkSink;
    reset();
  }

  public WatermarkCursor(
      WatermarkCache watermarkCache,
      WatermarkSink watermarkSink,
      String dataSource,
      Interval interval,
      DateTime offset
  )
  {
    this.dataSource = dataSource;
    this.interval = interval;
    this.watermarkCache = watermarkCache;
    this.watermarkSink = watermarkSink;
    reset();
    this.cursorTimestamp = offset;
  }

  /**
   * reset the cursor to init state
   */
  @Override
  public void reset()
  {
    DateTime seed = watermarkCache.get(dataSource, getType());
    this.cursorTimestamp = null;
    this.watermark = seed;
  }

  /**
   * "complete" the cursor, writing the cursor timestamp to the watermark sink if the cursor has mutated
   *
   * @return true
   */
  @Override
  public boolean complete()
  {
    log.debug("Completing %s watermark cursor.", getType());
    if (hasMutated()) {
      final DateTime ceil = getMetadata().minuteOfDay().roundCeilingCopy();
      log.info("Updating %s watermark for %s to %s", getType(), getDataSource(), ceil);
      watermark = ceil;
      watermarkCache.set(getDataSource(), getType(), ceil);
      watermarkSink.update(getDataSource(), getType(), ceil);
    }
    return true;
  }

  public boolean manualOverride(DateTime override)
  {
    this.cursorTimestamp = override;
    return this.complete();
  }

  /**
   * advance the watermark cursor, returning true if the cursor can continue, or completing and returning false
   * if processing is complete
   *
   * @param segmentDetails next segment to process
   *
   * @return true if cursor should continue advancing after this invocation
   */
  @Override
  public abstract boolean advance(TimelineSegment segmentDetails);

  @Override
  public abstract String getType();

  @Override
  public String getDataSource()
  {
    return dataSource;
  }

  @Override
  public Interval getInterval()
  {
    return interval;
  }

  @Override
  public DateTime getTimestamp()
  {
    return getMetadata();
  }

  /**
   * get the greater value of the cursor timestamp and current watermark
   *
   * @return the greater value of the cursor timestamp and current watermark
   */
  @Override
  public DateTime getMetadata()
  {
    return getWatermark() == null || (cursorTimestamp != null && cursorTimestamp.isAfter(getWatermark()))
           ? cursorTimestamp
           : getWatermark();
  }

  public DateTime getCursorTimestamp()
  {
    return cursorTimestamp;
  }

  public DateTime getWatermark()
  {
    return watermark;
  }

  protected WatermarkCache getWatermarkCache()
  {
    return watermarkCache;
  }

  protected WatermarkSink getWatermarkSink()
  {
    return watermarkSink;
  }

  /**
   * advance the timestamp to value if the current cursor timestamp is before the new value
   *
   * @param value
   *
   * @return true
   */
  protected boolean advanceTimestamp(DateTime value)
  {
    if (cursorTimestamp == null || cursorTimestamp.isBefore(value)) {
      cursorTimestamp = value;
    }
    return true;
  }

  /**
   * determine if cursor advancement has resulted in a need to update the watermark in the watermark cache
   *
   * @return boolean indicating whether the cursor has a mutated watermark
   */
  protected boolean hasMutated()
  {
    DateTime ts = getMetadata();
    return ts != null && !ts.minuteOfDay().roundCeilingCopy().equals(
        watermark != null ? watermark.minuteOfDay().roundCeilingCopy() : null
    );
  }

  /**
   * determine if a timeline segment is contiguous to the current cursor timestamp, or the existing watermark
   *
   * @param segmentDetails
   *
   * @return boolean indicating if segment is contiguous to cursor or existing watermark
   */
  protected boolean isContiguous(TimelineSegment segmentDetails)
  {
    return segmentDetails.isContiguous(getCursorTimestamp()) ||
           (getWatermark() != null && segmentDetails.isContiguous(getWatermark()));
  }

  /**
   * Check to determine if cursor should continue in order to seek to point of the existing watermark
   *
   * @param segmentDetails
   *
   * @return boolean indicating if cursor should continue to seek to existing watermark
   */
  protected boolean canSkip(TimelineSegment segmentDetails)
  {
    return getWatermark() != null && getWatermark().isAfter(segmentDetails.getInterval().getStart());
  }
}
