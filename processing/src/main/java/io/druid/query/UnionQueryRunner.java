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
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;

public class UnionQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner<T> baseRunner;
  private final QueryToolChest<T, Query<T>> toolChest;

  public UnionQueryRunner(
      QueryRunner<T> baseRunner,
      QueryToolChest<T, Query<T>> toolChest
  )
  {
    this.baseRunner = baseRunner;
    this.toolChest = toolChest;
  }

  @Override
  public Sequence<T> run(final Query<T> query)
  {
    DataSource dataSource = query.getDataSource();
    if (dataSource instanceof UnionDataSource) {
      return toolChest.mergeSequencesUnordered(
          Sequences.simple(
              Lists.transform(
                  ((UnionDataSource) dataSource).getDataSources(),
                  new Function<DataSource, Sequence<T>>()
                  {
                    @Override
                    public Sequence<T> apply(DataSource singleSource)
                    {
                      return baseRunner.run(
                          query.withDataSource(singleSource)
                      );
                    }
                  }
              )
          )
      );
    } else {
      return baseRunner.run(query);
    }
  }

}
