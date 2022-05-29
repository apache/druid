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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.queryng.fragment.QueryProfile.FragmentNode;
import org.apache.druid.queryng.fragment.QueryProfile.OperatorChildNode;
import org.apache.druid.queryng.fragment.QueryProfile.OperatorNode;
import org.apache.druid.queryng.fragment.QueryProfile.SliceNode;

import java.util.Map.Entry;

/**
 * Visualize an operator plan in an Impala-like text format.
 */
public class ProfileVisualizer
{
  private final String INDENT = "| ";
  private final String METRIC_INDENT = "  ";

  private final QueryProfile profile;
  private final StringBuilder buf = new StringBuilder();

  public ProfileVisualizer(QueryProfile profile)
  {
    this.profile = profile;
  }

  public String render()
  {
    buf.setLength(0);
    buf.append("----------\n")
       .append("Query ID: ")
       .append(profile.queryId)
       .append("\n")
       .append("Runtime (ms): ")
       .append(profile.runTimeMs)
       .append("\n");
    if (profile.nativeQuery != null) {
      buf.append("Query Type: ")
         .append(profile.nativeQuery.getClass().getSimpleName())
         .append("\n");
    }
    if (profile.error != null) {
      buf.append("Error: ")
         .append(profile.error.getClass().getSimpleName())
         .append(" - ")
         .append(profile.error.getMessage())
         .append("\n");
    }
    buf.append("\n");
    if (profile.slices.isEmpty()) {
      buf.append("No slices available in query.\n");
    } else {
      buf.append("-- Root Slice  --\n\n");
      renderFragment(profile.slices.get(0).fragments.get(0));
      for (int i = 1; i < profile.slices.size(); i++) {
        renderSlice(profile.slices.get(i));
      }
    }
    return buf.toString();
  }

  private void renderSlice(SliceNode slice)
  {
    buf.append("\n-- Slice ")
       .append(slice.sliceId)
       .append("  --\n");
    for (FragmentNode fragment : slice.fragments) {
      buf.append("\nFragment ")
         .append(fragment.fragmentId)
         .append("\n\n");
      renderFragment(fragment);
    }
  }

  private void renderFragment(FragmentNode fragment)
  {
    int i = 0;
    for (OperatorNode root : fragment.roots) {
      if (i > 0) {
        buf.append("\n");
      }
      renderNode(0, root);
      i++;
    }
  }

  private void renderNode(int level, OperatorNode node)
  {
    if (node.profile.omitFromProfile &&
        node.children != null &&
        node.children.size() == 1) {
      renderOperatorChild(level, node.children.get(0));
      return;
    }
    String indent = StringUtils.repeat(INDENT, level);
    buf.append(indent)
       .append(node.profile.operatorName)
       .append("\n");
    int childCount = node.children == null ? 0 : node.children.size();
    String innerIndent = indent + StringUtils.repeat(INDENT, childCount);
    for (Entry<String, Object> entry : node.profile.metrics().entrySet()) {
      buf.append(innerIndent)
         .append(METRIC_INDENT)
         .append(entry.getKey())
         .append(": ")
         .append(entry.getValue())
         .append("\n");
    }
    if (isLeaf(node)) {
      return;
    }
    buf.append(innerIndent).append("\n");
    for (int i = childCount - 1; i >= 0; i--) {
      renderOperatorChild(level + i, node.children.get(i));
    }
  }

  private void renderOperatorChild(int level, OperatorChildNode child)
  {
    if (child.operator == null) {
      String indent = StringUtils.repeat(INDENT, level);
      buf.append(indent)
         .append("Slice ")
         .append(child.slice)
         .append("\n");
    } else {
      renderNode(level, child.operator);
    }
  }

  private boolean isLeaf(OperatorNode node)
  {
    if (node.children == null || node.children.isEmpty()) {
      return true;
    }
    if (node.children.size() > 1) {
      return false;
    }
    OperatorChildNode child = node.children.get(0);
    if (child.operator == null) {
      return false;
    }
    return child.operator.profile.omitFromProfile && isLeaf(child.operator);
  }
}
