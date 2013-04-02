/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.aggregation.post;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 * Functionally similar to an Aggregator. See the Aggregator interface for more comments.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "arithmetic", value = ArithmeticPostAggregator.class),
    @JsonSubTypes.Type(name = "fieldAccess", value = FieldAccessPostAggregator.class),
    @JsonSubTypes.Type(name = "constant", value = ConstantPostAggregator.class)
})
public interface PostAggregator
{
  public Set<String> getDependentFields();

  public Comparator getComparator();

  public Object compute(Map<String, Object> combinedAggregators);

  public String getName();
}
