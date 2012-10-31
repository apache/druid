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

package com.metamx.druid.query.segment;

import java.util.List;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.joda.time.Interval;

import com.metamx.druid.Query;
import com.metamx.druid.query.QueryRunner;

/**
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, property="type", defaultImpl = LegacySegmentSpec.class)
@JsonSubTypes(value={
    @JsonSubTypes.Type(name="intervals", value=MultipleIntervalSegmentSpec.class),
    @JsonSubTypes.Type(name="segments", value=MultipleSpecificSegmentSpec.class)
})
public interface QuerySegmentSpec
{
  public List<Interval> getIntervals();

  public <T> QueryRunner<T> lookup(Query<T> query, QuerySegmentWalker walker);
}
