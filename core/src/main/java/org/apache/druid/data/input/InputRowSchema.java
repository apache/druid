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

package org.apache.druid.data.input;

import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;

import java.util.List;

/**
 * Schema of {@link InputRow}.
 */
public class InputRowSchema
{
  private final TimestampSpec timestampSpec;
  private final DimensionsSpec dimensionsSpec;
  private final List<String> metricNames;

  public InputRowSchema(TimestampSpec timestampSpec, DimensionsSpec dimensionsSpec, List<String> metricNames)
  {
    this.timestampSpec = timestampSpec;
    this.dimensionsSpec = dimensionsSpec;
    this.metricNames = metricNames;
  }

  public TimestampSpec getTimestampSpec()
  {
    return timestampSpec;
  }

  public DimensionsSpec getDimensionsSpec()
  {
    return dimensionsSpec;
  }

  public List<String> getMetricNames()
  {
    return metricNames;
  }
}
