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

package io.druid.jackson;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.druid.granularity.AllGranularity;
import io.druid.granularity.DurationGranularity;
import io.druid.granularity.NoneGranularity;
import io.druid.granularity.PeriodGranularity;
import io.druid.granularity.QueryGranularity;

/**
 */
public class QueryGranularityModule extends SimpleModule
{
  public QueryGranularityModule()
  {
    super("QueryGranularityModule");

    setMixInAnnotation(QueryGranularity.class, QueryGranularityMixin.class);
    registerSubtypes(
        new NamedType(PeriodGranularity.class, "period"),
        new NamedType(DurationGranularity.class, "duration"),
        new NamedType(AllGranularity.class, "all"),
        new NamedType(NoneGranularity.class, "none")
    );
  }

  @JsonTypeInfo(use= JsonTypeInfo.Id.NAME, property = "type", defaultImpl = QueryGranularity.class)
  public static interface QueryGranularityMixin {}
}
