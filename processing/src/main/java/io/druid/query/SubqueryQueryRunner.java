/*
 * Druid - a distributed column store.
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
 *
 * This file Copyright (C) 2014 N3TWORK, Inc. and contributed to the Druid project
 * under the Druid Corporate Contributor License Agreement.
 */

package io.druid.query;

import com.metamx.common.guava.Sequence;

import java.util.Map;

/**
 * If there's a subquery, run it instead of the outer query
 */
public class SubqueryQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner<T> baseRunner;

  public SubqueryQueryRunner(QueryRunner<T> baseRunner)
  {
    this.baseRunner = baseRunner;
  }

  @Override
  public Sequence<T> run(final Query<T> query, Map<String, Object> responseContext)
  {
    DataSource dataSource = query.getDataSource();
    if (dataSource instanceof QueryDataSource) {
      return run((Query<T>) ((QueryDataSource) dataSource).getQuery(), responseContext);
    } else {
      return baseRunner.run(query, responseContext);
    }
  }
}
