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

package org.apache.druid.segment.realtime.appenderator;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeTuningConfig;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.SegmentPublisher;
import org.apache.druid.segment.realtime.plumber.Plumber;
import org.apache.druid.segment.realtime.plumber.PlumberSchool;
import org.apache.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;

public class AppenderatorPlumberSchool implements PlumberSchool
{
  private final AppenderatorFactory appenderatorFactory;
  private final DataSegmentAnnouncer segmentAnnouncer;
  private final SegmentHandoffNotifierFactory handoffNotifierFactory;
  private final SegmentPublisher segmentPublisher;

  @JsonCreator
  public AppenderatorPlumberSchool(
      @JsonProperty("appenderator") AppenderatorFactory appenderatorFactory,
      @JacksonInject DataSegmentAnnouncer segmentAnnouncer,
      @JacksonInject SegmentHandoffNotifierFactory handoffNotifierFactory,
      @JacksonInject SegmentPublisher segmentPublisher
  )
  {
    this.appenderatorFactory = appenderatorFactory;
    this.segmentAnnouncer = segmentAnnouncer;
    this.handoffNotifierFactory = handoffNotifierFactory;
    this.segmentPublisher = segmentPublisher;
  }

  @Override
  public Plumber findPlumber(
      final DataSchema schema,
      final RealtimeTuningConfig config,
      final FireDepartmentMetrics metrics
  )
  {
    final Appenderator appenderator = appenderatorFactory.build(
        schema,
        config,
        metrics
    );

    return new AppenderatorPlumber(
        schema,
        config,
        metrics,
        segmentAnnouncer,
        segmentPublisher,
        handoffNotifierFactory.createSegmentHandoffNotifier(schema.getDataSource()),
        appenderator
    );
  }

  @JsonProperty("appenderator")
  public AppenderatorFactory getAppenderatorFactory()
  {
    return appenderatorFactory;
  }
}
