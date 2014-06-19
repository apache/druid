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

package io.druid.query;

import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.query.spec.MultipleSpecificSegmentSpec;
import io.druid.segment.SegmentMissingException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RetryQueryRunner<T> implements QueryRunner<T>
{
  public static String missingSegments = "missingSegments";
  private final QueryRunner<T> baseRunner;
  private final QueryToolChest<T, Query<T>> toolChest;
  private final RetryQueryRunnerConfig config;

  public RetryQueryRunner(QueryRunner<T> baseRunner, QueryToolChest<T, Query<T>> toolChest, RetryQueryRunnerConfig config)
  {
    this.baseRunner = baseRunner;
    this.toolChest = toolChest;
    this.config = config;
  }

  @Override
  public Sequence<T> run(final Query<T> query, Map<String, Object> context)
  {
    Sequence<T> returningSeq = baseRunner.run(query, context);


    for (int i = config.numTries(); i > 0 && !((List)context.get(missingSegments)).isEmpty(); i--) {
        List<SegmentDescriptor> segList= (List<SegmentDescriptor>)context.get(missingSegments);
        ((List)context.get(missingSegments)).clear();
        returningSeq = toolChest.mergeSequences(
            Sequences.simple(
                Arrays.asList(
                    returningSeq,
                    baseRunner.run(
                        query.withQuerySegmentSpec(new MultipleSpecificSegmentSpec(segList)),
                        context
                    )
                )
            )
        );
    }

    if (!config.returnPartialResults() && !((List)context.get(missingSegments)).isEmpty()) {
      String failedSegments = "";
      for (SegmentDescriptor segment : (List<SegmentDescriptor>) context.get("missingSegments")) {
        failedSegments = failedSegments + segment.toString() + " ";
      }
      throw new SegmentMissingException("The following segments are missing: " + failedSegments);
    }

    return returningSeq;
  }
}

