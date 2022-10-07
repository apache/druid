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

package org.apache.druid.queryng.fragment;

import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.Query;
import org.apache.druid.queryng.fragment.FragmentManager.OperatorChild;
import org.apache.druid.queryng.fragment.FragmentManager.OperatorTracker;
import org.apache.druid.queryng.fragment.QueryManager.FragmentTracker;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.OperatorProfile;
import org.apache.druid.queryng.operators.Operators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class QueryProfile
{
  protected static final EmittingLogger log = new EmittingLogger(QueryProfile.class);

  public static class SliceNode
  {
    public final int sliceId;
    public final List<FragmentNode> fragments;

    public SliceNode(int sliceId, List<FragmentNode> fragments)
    {
      super();
      this.sliceId = sliceId;
      this.fragments = fragments;
    }
  }

  /**
   * Profile for an operator fragment with operators organized into a tree.
   */
  public static class FragmentNode
  {
    public final String queryId;
    public final int fragmentId;
    public final long runTimeMs;
    public final Exception error;
    public final List<OperatorNode> roots;

    protected FragmentNode(
        FragmentManager fragment,
        int fragmentId,
        final List<OperatorNode> roots
    )
    {
      this.fragmentId = fragmentId;
      this.queryId = fragment.queryId();
      this.runTimeMs = fragment.elapsedTimeMs();
      this.error = fragment.exception();
      this.roots = roots;
    }
  }

  public static class OperatorNode
  {
    public final OperatorProfile profile;
    public final List<OperatorChildNode> children;

    protected OperatorNode(
        final OperatorProfile operatorProfile,
        final List<OperatorChildNode> children)
    {
      this.profile = operatorProfile;
      this.children = children == null || children.isEmpty() ? null : children;
    }
  }

  public static class OperatorChildNode
  {
    public final OperatorNode operator;
    public final int slice;

    public OperatorChildNode(OperatorNode operator, int slice)
    {
      super();
      this.operator = operator;
      this.slice = slice;
    }
  }

  private static class Builder
  {
    private final QueryManager query;
    private Exception error;
    private final List<SliceNode> slices = new ArrayList<>();

    public Builder(QueryManager query)
    {
      this.query = query;
    }

    public QueryProfile build()
    {
      error = query.rootFragment().exception();
      profileSlices();
      return new QueryProfile(this);
    }

    private void profileSlices()
    {
      Map<FragmentManager, FragmentTracker> allFragments = query.fragments();
      if (allFragments.size() == 1) {
        return;
      }
      Map<Integer, List<FragmentTracker>> slices = new HashMap<>();
      for (FragmentTracker tracker : allFragments.values()) {
        List<FragmentTracker> fragments = slices.get(tracker.sliceId);
        if (fragments == null) {
          fragments = new ArrayList<>();
          slices.put(tracker.sliceId, fragments);
        }
        fragments.add(tracker);
      }
      List<Integer> keys = new ArrayList<>(slices.keySet());
      Collections.sort(keys);
      for (Integer key : keys) {
        this.slices.add(profileSlice(key, slices.get(key)));
      }
    }

    private SliceNode profileSlice(int sliceId, List<FragmentTracker> fragments)
    {
      List<FragmentNode> fragmentNodes = new ArrayList<>();
      Collections.sort(fragments, (f1, f2) -> Integer.compare(f1.instanceId, f2.instanceId));
      for (int i = 0; i < fragments.size(); i++) {
        fragmentNodes.add(profileFragment(i + 1, fragments.get(i).fragment));
      }
      return new SliceNode(sliceId, fragmentNodes);
    }

    public FragmentNode profileFragment(int fragmentId, FragmentManager fragment)
    {
      Map<Operator<?>, OperatorTracker> operators = fragment.operators();
      Map<Operator<?>, Boolean> rootCandidates = new IdentityHashMap<>();
      for (Operator<?> op : operators.keySet()) {
        rootCandidates.put(op, true);
      }
      for (Entry<Operator<?>, OperatorTracker> entry : operators.entrySet()) {
        for (OperatorChild child : entry.getValue().children) {
          if (child.operator != null) {
            rootCandidates.put(child.operator, false);
          }
        }
      }
      List<OperatorNode> rootProfiles = new ArrayList<>();
      Operator<?> root;
      if (fragment.rootIsOperator()) {
        root = fragment.rootOperator();
      } else if (fragment.rootIsSequence()) {
        root = Operators.unwrapOperator(fragment.rootSequence());
      } else {
        root = null;
      }
      if (root != null) {
        rootCandidates.put(root, false);
        rootProfiles.add(profileOperator(operators, root));
      }
      for (Entry<Operator<?>, Boolean> entry : rootCandidates.entrySet()) {
        if (entry.getValue()) {
          rootProfiles.add(profileOperator(operators, entry.getKey()));
        }
      }
      return new FragmentNode(fragment, fragmentId, rootProfiles);
    }

    private OperatorNode profileOperator(
        Map<Operator<?>, OperatorTracker> operators,
        Operator<?> root
    )
    {
      List<OperatorChildNode> childProfiles;
      OperatorProfile rootProfile;
      OperatorTracker tracker = operators.get(root);
      if (tracker == null) {
        childProfiles = null;
        rootProfile = null;
      } else {
        List<OperatorChild> orderedChildren = new ArrayList<>(tracker.children);
        Collections.sort(orderedChildren, (a, b) -> Integer.compare(a.position, b.position));
        // Sanity check
        for (int i = 0; i < orderedChildren.size(); i++) {
          if (orderedChildren.get(i).position != i) {
            log.warn(
                "Operator %s: child at index %d has position %d",
                root.getClass().getSimpleName(),
                i,
                orderedChildren.get(i).position
            );
          }
        }
        childProfiles = new ArrayList<>();
        for (OperatorChild child : orderedChildren) {
          if (child.operator == null) {
            childProfiles.add(new OperatorChildNode(null, child.sliceID));
          } else {
            childProfiles.add(
                new OperatorChildNode(profileOperator(operators, child.operator), 0));
          }
        }
        rootProfile = tracker.profile;
      }
      if (rootProfile == null) {
        rootProfile = new OperatorProfile(root.getClass().getSimpleName());
      }
      return new OperatorNode(
          rootProfile,
          childProfiles
      );
    }
  }

  public final Query<?> nativeQuery;
  public final String queryId;
  public final long runTimeMs;
  public final Exception error;
  public final List<SliceNode> slices;

  public static QueryProfile build(QueryManager query)
  {
    return new Builder(query).build();
  }

  private QueryProfile(Builder builder)
  {
    this.nativeQuery = builder.query.rootQuery();
    this.queryId = builder.query.queryId();
    this.runTimeMs = builder.query.runTimeMs();
    this.error = builder.error;
    this.slices = builder.slices;
  }
}
