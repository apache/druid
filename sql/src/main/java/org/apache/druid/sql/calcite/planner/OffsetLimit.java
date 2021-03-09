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

package org.apache.druid.sql.calcite.planner;

import org.apache.druid.com.google.common.base.Preconditions;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Represents an offset and a limit.
 */
public class OffsetLimit
{
  private final long offset;

  @Nullable
  private final Long limit;

  OffsetLimit(long offset, @Nullable Long limit)
  {
    Preconditions.checkArgument(offset >= 0, "offset >= 0");
    Preconditions.checkArgument(limit == null || limit >= 0, "limit == null || limit >= 0");

    this.offset = offset;
    this.limit = limit;
  }

  public static OffsetLimit none()
  {
    return new OffsetLimit(0, null);
  }

  public static OffsetLimit fromSort(Sort sort)
  {
    final long offset = sort.offset == null ? 0 : (long) RexLiteral.intValue(sort.offset);
    final Long limit = sort.fetch == null ? null : (long) RexLiteral.intValue(sort.fetch);
    return new OffsetLimit(offset, limit);
  }

  public boolean hasOffset()
  {
    return offset > 0;
  }

  public long getOffset()
  {
    return offset;
  }

  public boolean hasLimit()
  {
    return limit != null;
  }

  public long getLimit()
  {
    Preconditions.checkState(limit != null, "limit is not present");
    return limit;
  }

  @Nullable
  public RexNode getOffsetAsRexNode(final RexBuilder builder)
  {
    if (offset == 0) {
      return null;
    } else {
      return builder.makeLiteral(
          offset,
          new BasicSqlType(DruidTypeSystem.INSTANCE, SqlTypeName.BIGINT),
          false
      );
    }
  }

  @Nullable
  public RexNode getLimitAsRexNode(final RexBuilder builder)
  {
    if (limit == null) {
      return null;
    } else {
      return builder.makeLiteral(
          limit,
          new BasicSqlType(DruidTypeSystem.INSTANCE, SqlTypeName.BIGINT),
          false
      );
    }
  }

  /**
   * Return a new instance that represents applying this one first, then the {@param next} one.
   */
  public OffsetLimit andThen(final OffsetLimit next)
  {
    final Long newLimit;

    if (limit == null && next.limit == null) {
      // Neither has a limit => no limit overall.
      newLimit = null;
    } else if (limit == null) {
      // Outer limit only.
      newLimit = next.limit;
    } else if (next.limit == null) {
      // Inner limit only.
      newLimit = Math.max(0, limit - next.offset);
    } else {
      // Both outer and inner limit.
      newLimit = Math.max(0, Math.min(limit - next.offset, next.limit));
    }

    return new OffsetLimit(offset + next.offset, newLimit);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OffsetLimit that = (OffsetLimit) o;
    return Objects.equals(offset, that.offset) &&
           Objects.equals(limit, that.limit);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(offset, limit);
  }

  @Override
  public String toString()
  {
    return "OffsetLimit{" +
           "offset=" + offset +
           ", limit=" + limit +
           '}';
  }
}
