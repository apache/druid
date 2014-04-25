/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server.coordinator.rules;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "loadByPeriod", value = PeriodLoadRule.class),
    @JsonSubTypes.Type(name = "loadByInterval", value = IntervalLoadRule.class),
    @JsonSubTypes.Type(name = "loadForever", value = ForeverLoadRule.class),
    @JsonSubTypes.Type(name = "dropByPeriod", value = PeriodDropRule.class),
    @JsonSubTypes.Type(name = "dropByInterval", value = IntervalDropRule.class),
    @JsonSubTypes.Type(name = "dropForever", value = ForeverDropRule.class)
})

public interface Rule
{
  public String getType();

  public boolean appliesTo(DataSegment segment, DateTime referenceTimestamp);

  public boolean appliesTo(Interval interval, DateTime referenceTimestamp);

  public CoordinatorStats run(DruidCoordinator coordinator, DruidCoordinatorRuntimeParams params, DataSegment segment);
}
