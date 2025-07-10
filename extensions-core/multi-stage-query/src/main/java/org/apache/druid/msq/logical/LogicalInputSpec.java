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

package org.apache.druid.msq.logical;

import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.kernel.StageDefinitionBuilder;
import org.apache.druid.msq.logical.stages.LogicalStage;
import org.apache.druid.msq.querykit.InputNumberDataSource;
import org.apache.druid.query.DataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.querygen.SourceDescProducer.SourceDesc;

import java.util.Collections;
import java.util.Set;

/**
 * Represents an {@link InputSpec} for {@link LogicalStage}-s.
 */
public abstract class LogicalInputSpec
{
  public interface InputProperty
  {
    InputProperty BROADCAST = new Broadcast();
  }

  private static final class Broadcast implements InputProperty
  {
    private Broadcast()
    {
    }
  }

  final int inputIndex;
  final Set<InputProperty> props;

  public LogicalInputSpec(int inputIndex, Set<InputProperty> props)
  {
    this.inputIndex = inputIndex;
    this.props = props;
  }

  public abstract InputSpec toInputSpec(StageMaker maker);

  public abstract RowSignature getRowSignature();

  /**
   * Provides the {@link SourceDesc} for this input spec.
   *
   * Supplied to make it more easily interoperable with {@link DataSource}
   * backed features like {@link DataSource#createSegmentMapFunction}.
   */
  public final SourceDesc getSourceDesc()
  {
    InputNumberDataSource ds = new InputNumberDataSource(inputIndex);
    return new SourceDesc(ds, getRowSignature());
  }

  public final boolean hasProperty(InputProperty prop)
  {
    return props.contains(prop);
  }

  public static LogicalInputSpec of(LogicalStage inputStage)
  {
    return of(inputStage, 0, Collections.emptySet());
  }

  public static LogicalInputSpec of(InputSpec inputSpec, RowSignature rowSignature)
  {
    return new PhysicalInputSpec(inputSpec, 0, rowSignature, Collections.emptySet());
  }

  public static LogicalInputSpec of(LogicalStage logicalStage, int inputIndex, InputProperty prop)
  {
    return of(logicalStage, inputIndex, Collections.singleton(prop));
  }

  public static LogicalInputSpec of(LogicalStage logicalStage, int inputIndex, Set<InputProperty> props)
  {
    // could potentially unwrap LogicalStage if some conditions are met
    // logicalStage.unwrap(InputSpec.class);
    // partial: https://github.com/kgyrtkirk/druid/commit/9a541f69361f341c537ee196514d2d6a00ae3feb
    return new DagStageInputSpec(logicalStage, inputIndex, props);
  }

  static class PhysicalInputSpec extends LogicalInputSpec
  {
    private InputSpec inputSpec;
    private RowSignature rowSignature;

    public PhysicalInputSpec(InputSpec inputSpec, int inputIndex, RowSignature rowSignature, Set<InputProperty> props)
    {
      super(inputIndex, props);
      this.inputSpec = inputSpec;
      this.rowSignature = rowSignature;
    }

    @Override
    public InputSpec toInputSpec(StageMaker maker)
    {
      return inputSpec;
    }

    @Override
    public RowSignature getRowSignature()
    {
      return rowSignature;
    }
  }

  static class DagStageInputSpec extends LogicalInputSpec
  {
    protected LogicalStage inputStage;

    public DagStageInputSpec(LogicalStage inputStage, int inputIndex, Set<InputProperty> props)
    {
      super(inputIndex, props);
      this.inputStage = inputStage;
    }

    @Override
    public InputSpec toInputSpec(StageMaker maker)
    {
      StageDefinitionBuilder stage = maker.buildStage(inputStage);
      return new StageInputSpec(stage.getStageNumber());
    }

    public LogicalStage getStage()
    {
      return inputStage;
    }

    @Override
    public RowSignature getRowSignature()
    {
      return inputStage.getLogicalRowSignature();
    }
  }
}
