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

package org.apache.druid.extensions.watermarking.storage.composite;

import com.google.inject.Inject;
import org.apache.druid.extensions.watermarking.storage.WatermarkSink;
import org.apache.druid.java.util.common.logger.Logger;
import org.joda.time.DateTime;

import java.util.List;

public class CompositeWatermarkSink implements WatermarkSink
{
  private static final Logger log = new Logger(CompositeWatermarkSink.class);

  private final List<WatermarkSink> sinks;

  @Inject
  public CompositeWatermarkSink(
      List<WatermarkSink> sinks
  )
  {
    this.sinks = sinks;
  }

  public List<WatermarkSink> getSinks()
  {
    return sinks;
  }

  @Override
  public void update(String datasource, String type, DateTime timestamp)
  {
    for (WatermarkSink sink : sinks) {
      try {
        sink.update(datasource, type, timestamp);
      }
      catch (Exception ex) {
        log.error(ex, "Failed to update sink %s", sink.getClass().getCanonicalName());
      }
    }
  }

  @Override
  public void rollback(String datasource, String type, DateTime timestamp)
  {
    for (WatermarkSink sink : sinks) {
      try {
        sink.rollback(datasource, type, timestamp);
      }
      catch (Exception ex) {
        log.error(ex, "Failed to rollback sink %s", sink.getClass().getCanonicalName());
      }
    }
  }

  @Override
  public void purgeHistory(String datasource, String type, DateTime timestamp)
  {
    for (WatermarkSink sink : sinks) {
      try {
        sink.purgeHistory(datasource, type, timestamp);
      }
      catch (Exception ex) {
        log.error(ex, "Failed to purge history for sink %s", sink.getClass().getCanonicalName());
      }
    }
  }

  @Override
  public void initialize()
  {
    for (WatermarkSink sink : sinks) {
      try {
        sink.initialize();
      }
      catch (Exception ex) {
        log.error(ex, "Failed to initialize sink %s", sink.getClass().getCanonicalName());
      }
    }
  }
}
