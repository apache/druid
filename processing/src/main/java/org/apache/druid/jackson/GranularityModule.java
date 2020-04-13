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

package org.apache.druid.jackson;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.druid.java.util.common.granularity.AllGranularity;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.NoneGranularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;

public class GranularityModule extends SimpleModule
{

  public GranularityModule()
  {
    super("GranularityModule");

    setMixInAnnotation(Granularity.class, GranularityMixin.class);
    registerSubtypes(
        new NamedType(PeriodGranularity.class, "period"),
        new NamedType(DurationGranularity.class, "duration"),
        new NamedType(AllGranularity.class, "all"),
        new NamedType(NoneGranularity.class, "none")
    );
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = Granularity.class)
  public interface GranularityMixin
  {
  }
}
