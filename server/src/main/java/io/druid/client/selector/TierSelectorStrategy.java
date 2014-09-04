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

package io.druid.client.selector;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.timeline.DataSegment;

import java.util.Comparator;
import java.util.Set;
import java.util.TreeMap;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "tier", defaultImpl = HighestPriorityTierSelectorStrategy.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "highestPriority", value = HighestPriorityTierSelectorStrategy.class),
    @JsonSubTypes.Type(name = "lowestPriority", value = LowestPriorityTierSelectorStrategy.class),
    @JsonSubTypes.Type(name = "custom", value = CustomTierSelectorStrategy.class)
})
public interface TierSelectorStrategy
{
  public Comparator<Integer> getComparator();

  public QueryableDruidServer pick(TreeMap<Integer, Set<QueryableDruidServer>> prioritizedServers, DataSegment segment);
}
