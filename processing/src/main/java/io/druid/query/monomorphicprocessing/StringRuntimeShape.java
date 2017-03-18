/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.monomorphicprocessing;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Class to be used to obtain String representation of runtime shape of one or several {@link HotLoopCallee}s.
 * Example:
 *
 * String offsetShape = StringRuntimeShape.of(offset);
 * String dimensionSelectorShape = StringRuntimeShape.of(dimensionSelector);
 */
public final class StringRuntimeShape
{

  public static String of(HotLoopCallee hotLoopCallee)
  {
    return new Inspector().runtimeShapeOf(hotLoopCallee);
  }

  public static String of(HotLoopCallee... hotLoopCallees)
  {
    return new Inspector().runtimeShapeOf(hotLoopCallees);
  }

  private StringRuntimeShape()
  {
  }

  private static class Inspector implements RuntimeShapeInspector
  {
    private final StringBuilder sb = new StringBuilder();
    private int indent = 0;

    String runtimeShapeOf(HotLoopCallee hotLoopCallee)
    {
      visit(hotLoopCallee);
      return sb.toString();
    }

    String runtimeShapeOf(HotLoopCallee[] hotLoopCallees)
    {
      for (HotLoopCallee hotLoopCallee : hotLoopCallees) {
        visit(hotLoopCallee);
        sb.append(",\n");
      }
      sb.setLength(sb.length() - 2);
      return sb.toString();
    }

    private void indent()
    {
      for (int i = 0; i < indent; i++) {
        sb.append(' ');
      }
    }

    private void incrementIndent()
    {
      indent += 2;
    }

    private void decrementIndent()
    {
      indent -= 2;
    }

    private void visit(@Nullable Object value)
    {
      if (value == null) {
        sb.append("null");
        return;
      }
      sb.append(value.getClass().getName());
      if (value instanceof HotLoopCallee) {
        appendHotLoopCalleeShape((HotLoopCallee) value);
      } else if (value instanceof ByteBuffer) {
        // ByteBuffers are treated specially because the byte order is an important part of the runtime shape.
        appendByteBufferShape((ByteBuffer) value);
      }
    }

    private void appendHotLoopCalleeShape(HotLoopCallee value)
    {
      sb.append(" {\n");
      int lengthBeforeInspection = sb.length();
      incrementIndent();
      value.inspectRuntimeShape(this);
      decrementIndent();
      if (sb.length() == lengthBeforeInspection) {
        // remove " {\n"
        sb.setLength(lengthBeforeInspection - 3);
      } else {
        removeLastComma();
        indent();
        sb.append('}');
      }
    }

    private void appendByteBufferShape(ByteBuffer byteBuffer)
    {
      sb.append(" {order: ");
      sb.append(byteBuffer.order().toString());
      sb.append('}');
    }

    private void removeLastComma()
    {
      assert sb.charAt(sb.length() - 2) == ',' && sb.charAt(sb.length() - 1) == '\n';
      sb.setCharAt(sb.length() - 2, '\n');
      sb.setLength(sb.length() - 1);
    }

    @Override
    public void visit(String fieldName, @Nullable HotLoopCallee value)
    {
      visit(fieldName, (Object) value);
    }

    @Override
    public void visit(String fieldName, @Nullable Object value)
    {
      indent();
      sb.append(fieldName);
      sb.append(": ");
      visit(value);
      sb.append(",\n");
    }

    @Override
    public <T> void visit(String fieldName, T[] values)
    {
      indent();
      sb.append(fieldName);
      sb.append(": [\n");
      int lengthBeforeInspection = sb.length();
      incrementIndent();
      for (T value : values) {
        indent();
        visit(value);
        sb.append(",\n");
      }
      decrementIndent();
      if (sb.length() == lengthBeforeInspection) {
        sb.setCharAt(lengthBeforeInspection - 1, ']');
      } else {
        removeLastComma();
        indent();
        sb.append(']');
      }
      sb.append(",\n");
    }

    @Override
    public void visit(String flagName, boolean flagValue)
    {
      indent();
      sb.append(flagName);
      sb.append(": ");
      sb.append(flagValue);
      sb.append(",\n");
    }

    @Override
    public void visit(String key, String runtimeShape)
    {
      indent();
      sb.append(key);
      sb.append(": ");
      sb.append(runtimeShape);
      sb.append(",\n");
    }
  }
}
