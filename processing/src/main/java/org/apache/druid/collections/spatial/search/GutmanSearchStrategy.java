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

package org.apache.druid.collections.spatial.search;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.spatial.ImmutableFloatPoint;
import org.apache.druid.collections.spatial.ImmutableNode;

/**
 */
public class GutmanSearchStrategy<TCoordinateArray, TPoint extends ImmutableNode<TCoordinateArray>> implements SearchStrategy<TCoordinateArray, TPoint>
{
  @Override
  public Iterable<ImmutableBitmap> search(ImmutableNode<TCoordinateArray> node, Bound<TCoordinateArray, TPoint> bound)
  {
    if (bound.getLimit() > 0) {
      return Iterables.transform(
          breadthFirstSearch(node, bound),
          new Function<>()
          {
            @Override
            public ImmutableBitmap apply(ImmutableNode immutableNode)
            {
              return immutableNode.getImmutableBitmap();
            }
          }
      );
    }

    return Iterables.transform(
        depthFirstSearch(node, bound),
        new Function<>()
        {
          @Override
          public ImmutableBitmap apply(ImmutableFloatPoint immutablePoint)
          {
            return immutablePoint.getImmutableBitmap();
          }
        }
    );
  }

  public Iterable<ImmutableFloatPoint> depthFirstSearch(ImmutableNode node, final Bound bound)
  {
    if (node.isLeaf()) {
      return bound.filter(
          Iterables.transform(
              node.getChildren(),
              new Function<ImmutableNode, ImmutableFloatPoint>()
              {
                @Override
                public ImmutableFloatPoint apply(ImmutableNode tNode)
                {
                  return new ImmutableFloatPoint(tNode);
                }
              }
          )
      );
    } else {
      return Iterables.concat(
          Iterables.transform(
              Iterables.filter(
                  node.getChildren(),
                  new Predicate<ImmutableNode>()
                  {
                    @Override
                    public boolean apply(ImmutableNode child)
                    {
                      return bound.overlaps(child);
                    }
                  }
              ),
              new Function<ImmutableNode, Iterable<ImmutableFloatPoint>>()
              {
                @Override
                public Iterable<ImmutableFloatPoint> apply(ImmutableNode child)
                {
                  return depthFirstSearch(child, bound);
                }
              }
          )
      );
    }
  }

  public Iterable<ImmutableNode> breadthFirstSearch(
      ImmutableNode node,
      final Bound bound
  )
  {
    if (node.isLeaf()) {
      return Iterables.filter(
          node.getChildren(),
          new Predicate<ImmutableNode>()
          {
            @Override
            public boolean apply(ImmutableNode immutableNode)
            {
              return bound.contains(immutableNode.getMinCoordinates());
            }
          }
      );
    }
    return breadthFirstSearch(node.getChildren(), bound, 0);
  }

  public Iterable<ImmutableNode> breadthFirstSearch(
      Iterable<ImmutableNode> nodes,
      final Bound bound,
      int total
  )
  {
    Iterable<ImmutableNode> points = Iterables.concat(
        Iterables.transform(
            Iterables.filter(
                nodes,
                new Predicate<>()
                {
                  @Override
                  public boolean apply(ImmutableNode immutableNode)
                  {
                    return immutableNode.isLeaf();
                  }
                }
            ),
            new Function<ImmutableNode, Iterable<ImmutableNode>>()
            {
              @Override
              public Iterable<ImmutableNode> apply(ImmutableNode immutableNode)
              {
                return Iterables.filter(
                    immutableNode.getChildren(),
                    new Predicate<ImmutableNode>()
                    {
                      @Override
                      public boolean apply(ImmutableNode immutableNode)
                      {
                        return bound.contains(immutableNode.getMinCoordinates());
                      }
                    }
                );
              }
            }
        )
    );

    Iterable<ImmutableNode> overlappingNodes = Iterables.filter(
        nodes,
        new Predicate<>()
        {
          @Override
          public boolean apply(ImmutableNode immutableNode)
          {
            return !immutableNode.isLeaf() && bound.overlaps(immutableNode);
          }
        }
    );

    int totalPoints = Iterables.size(points);
    int totalOverlap = Iterables.size(overlappingNodes);

    if (totalOverlap == 0 || (totalPoints + totalOverlap + total) >= bound.getLimit()) {
      return Iterables.concat(
          points,
          overlappingNodes
      );
    } else {
      return Iterables.concat(
          points,
          breadthFirstSearch(
              Iterables.concat(
                  Iterables.transform(
                      overlappingNodes,
                      new Function<ImmutableNode, Iterable<ImmutableNode>>()
                      {
                        @Override
                        public Iterable<ImmutableNode> apply(ImmutableNode immutableNode)
                        {
                          return immutableNode.getChildren();
                        }
                      }
                  )
              ),
              bound,
              totalPoints
          )
      );
    }
  }
}
