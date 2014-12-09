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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;

import java.util.Map;

public class UnionQueryRunner<T> implements QueryRunner<T>
{
  private final Iterable<QueryRunner> baseRunners;
  private final QueryToolChest<T, Query<T>> toolChest;

  public UnionQueryRunner(
      Iterable<QueryRunner> baseRunners,
      QueryToolChest<T, Query<T>> toolChest
  )
  {
    this.baseRunners = baseRunners;
    this.toolChest = toolChest;
  }

  @Override
  public Sequence<T> run(final Query<T> query, final Map<String, Object> responseContext)
  {
    if (Iterables.size(baseRunners) == 1) {
      return Iterables.getOnlyElement(baseRunners).run(query, responseContext);
    } else {
      return toolChest.mergeSequencesUnordered(
          Sequences.simple(
              Iterables.transform(
                  baseRunners,
                  new Function<QueryRunner, Sequence<T>>()
                  {
                    @Override
                    public Sequence<T> apply(QueryRunner singleRunner)
                    {
                      return singleRunner.run(
                          query,
                          responseContext
                      );
                    }
                  }
              )
          )
      );
    }
  }

}
