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

package org.apache.druid.segment.incremental;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.RowBasedColumnSelectorFactory;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.LongColumnSelector;
import org.apache.druid.segment.FloatColumnSelector;
import org.apache.druid.segment.DoubleColumnSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AggsManager<AggregatorType>
{

  protected final AggregatorFactory[] metrics;
  protected final AggregatorType[] aggs;
  protected final boolean deserializeComplexMetrics;
  protected final boolean reportParseExceptions;
  private final Map<String, ColumnCapabilitiesImpl> columnCapabilities;
  private final VirtualColumns virtualColumns;

  protected final Map<String, MetricDesc> metricDescs;
  public IncrementalIndex incrementalIndex;

  public final ReentrantLock[] aggLocks;

  public AggsManager(
          final IncrementalIndexSchema incrementalIndexSchema,
          final boolean deserializeComplexMetrics,
          final boolean reportParseExceptions,
          final boolean concurrentEventAdd,
          Supplier<InputRow> rowSupplier,
          Map<String, ColumnCapabilitiesImpl> columnCapabilities,
          IncrementalIndex incrementalIndex
  )
  {
    this.incrementalIndex = incrementalIndex;
    this.virtualColumns = incrementalIndexSchema.getVirtualColumns();
    this.metrics = incrementalIndexSchema.getMetrics();
    this.deserializeComplexMetrics = deserializeComplexMetrics;
    this.reportParseExceptions = reportParseExceptions;
    this.columnCapabilities = columnCapabilities;
    this.aggs = initAggs(metrics, rowSupplier, deserializeComplexMetrics, concurrentEventAdd);
    this.aggLocks = new ReentrantLock[this.aggs.length];
    for (int i = 0; i < this.aggLocks.length; i++) {
      this.aggLocks[i] = new ReentrantLock(true);
    }
    this.metricDescs = Maps.newLinkedHashMap();
    for (AggregatorFactory metric : metrics) {
      MetricDesc metricDesc = new MetricDesc(metricDescs.size(), metric);
      metricDescs.put(metricDesc.getName(), metricDesc);
      this.columnCapabilities.put(metricDesc.getName(), metricDesc.getCapabilities());
    }
  }

  protected abstract AggregatorType[] initAggs(
          AggregatorFactory[] metrics,
          Supplier<InputRow> rowSupplier,
          boolean deserializeComplexMetrics,
          boolean concurrentEventAdd
  );

  public AggregatorType[] getAggs()
  {
    return aggs;
  }

  public AggregatorFactory[] getMetricAggs()
  {
    return metrics;
  }

  @Nullable
  public String getMetricType(String metric)
  {
    final MetricDesc metricDesc = metricDescs.get(metric);
    return metricDesc != null ? metricDesc.getType() : null;
  }

  public List<String> getMetricNames()
  {
    return ImmutableList.copyOf(metricDescs.keySet());
  }

  public static AggregatorFactory[] getCombiningAggregators(AggregatorFactory[] aggregators)
  {
    AggregatorFactory[] combiningAggregators = new AggregatorFactory[aggregators.length];
    for (int i = 0; i < aggregators.length; i++) {
      combiningAggregators[i] = aggregators[i].getCombiningFactory();
    }
    return combiningAggregators;
  }

  protected ColumnSelectorFactory makeColumnSelectorFactory(
          final AggregatorFactory agg,
          final Supplier<InputRow> in,
          final boolean deserializeComplexMetrics
  )
  {
    return makeColumnSelectorFactory(virtualColumns, agg, in, deserializeComplexMetrics);
  }

  /**
   * Column selector used at ingestion time for inputs to aggregators.
   *
   * @param agg                       the aggregator
   * @param in                        ingestion-time input row supplier
   * @param deserializeComplexMetrics whether complex objects should be deserialized by a {@link ComplexMetricExtractor}
   *
   * @return column selector factory
   */
  public static ColumnSelectorFactory makeColumnSelectorFactory(
          final VirtualColumns virtualColumns,
          final AggregatorFactory agg,
          final Supplier<InputRow> in,
          final boolean deserializeComplexMetrics
  )
  {
    final RowBasedColumnSelectorFactory baseSelectorFactory = RowBasedColumnSelectorFactory.create(in, null);

    class IncrementalIndexInputRowColumnSelectorFactory implements ColumnSelectorFactory
    {
      @Override
      public ColumnValueSelector<?> makeColumnValueSelector(final String column)
      {
        final String typeName = agg.getTypeName();
        boolean isComplexMetric =
                GuavaUtils.getEnumIfPresent(ValueType.class, StringUtils.toUpperCase(typeName)) == null ||
                        typeName.equalsIgnoreCase(ValueType.COMPLEX.name());
        if (!isComplexMetric || !deserializeComplexMetrics) {
          return baseSelectorFactory.makeColumnValueSelector(column);
        } else {
          final ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(typeName);
          if (serde == null) {
            throw new ISE("Don't know how to handle type[%s]", typeName);
          }

          final ComplexMetricExtractor extractor = serde.getExtractor();
          return new ColumnValueSelector()
          {
            @Override
            public boolean isNull()
            {
              return in.get().getMetric(column) == null;
            }

            @Override
            public long getLong()
            {
              Number metric = in.get().getMetric(column);
              assert NullHandling.replaceWithDefault() || metric != null;
              return DimensionHandlerUtils.nullToZero(metric).longValue();
            }

            @Override
            public float getFloat()
            {
              Number metric = in.get().getMetric(column);
              assert NullHandling.replaceWithDefault() || metric != null;
              return DimensionHandlerUtils.nullToZero(metric).floatValue();
            }

            @Override
            public double getDouble()
            {
              Number metric = in.get().getMetric(column);
              assert NullHandling.replaceWithDefault() || metric != null;
              return DimensionHandlerUtils.nullToZero(metric).doubleValue();
            }

            @Override
            public Class classOfObject()
            {
              return extractor.extractedClass();
            }

            @Override
            public Object getObject()
            {
              return extractor.extractValue(in.get(), column);
            }

            @Override
            public void inspectRuntimeShape(RuntimeShapeInspector inspector)
            {
              inspector.visit("in", in);
              inspector.visit("extractor", extractor);
            }
          };
        }
      }

      @Override
      public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
      {
        return baseSelectorFactory.makeDimensionSelector(dimensionSpec);
      }

      @Nullable
      @Override
      public ColumnCapabilities getColumnCapabilities(String columnName)
      {
        return baseSelectorFactory.getColumnCapabilities(columnName);
      }
    }

    return virtualColumns.wrap(new IncrementalIndexInputRowColumnSelectorFactory());
  }

  private class LongMetricColumnSelector implements LongColumnSelector
  {
    private final IncrementalIndexRowHolder currEntry;
    private final int metricIndex;

    public LongMetricColumnSelector(IncrementalIndexRowHolder currEntry, int metricIndex)
    {
      this.currEntry = currEntry;
      this.metricIndex = metricIndex;
    }

    @Override
    public long getLong()
    {
      assert NullHandling.replaceWithDefault() || !isNull();
      return AggsManager.this.incrementalIndex.getMetricLongValue(currEntry.get(), metricIndex);
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("index", AggsManager.this.incrementalIndex);
    }

    @Override
    public boolean isNull()
    {
      return AggsManager.this.incrementalIndex.isNull(currEntry.get(), metricIndex);
    }
  }

  private class ObjectMetricColumnSelector extends ObjectColumnSelector
  {
    private final IncrementalIndexRowHolder currEntry;
    private final int metricIndex;
    private Class classOfObject;

    public ObjectMetricColumnSelector(
            MetricDesc metricDesc,
            IncrementalIndexRowHolder currEntry,
            int metricIndex
    )
    {
      this.currEntry = currEntry;
      this.metricIndex = metricIndex;
      classOfObject = ComplexMetrics.getSerdeForType(metricDesc.getType()).getObjectStrategy().getClazz();
    }

    @Nullable
    @Override
    public Object getObject()
    {
      return AggsManager.this.incrementalIndex.getMetricObjectValue(currEntry.get(), metricIndex);
    }

    @Override
    public Class classOfObject()
    {
      return classOfObject;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("index", AggsManager.this.incrementalIndex);
    }
  }

  private class FloatMetricColumnSelector implements FloatColumnSelector
  {
    private final IncrementalIndexRowHolder currEntry;
    private final int metricIndex;

    public FloatMetricColumnSelector(IncrementalIndexRowHolder currEntry, int metricIndex)
    {
      this.currEntry = currEntry;
      this.metricIndex = metricIndex;
    }

    @Override
    public float getFloat()
    {
      assert NullHandling.replaceWithDefault() || !isNull();
      return AggsManager.this.incrementalIndex.getMetricFloatValue(currEntry.get(), metricIndex);
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("index", AggsManager.this.incrementalIndex);
    }

    @Override
    public boolean isNull()
    {
      return AggsManager.this.incrementalIndex.isNull(currEntry.get(), metricIndex);
    }
  }

  private class DoubleMetricColumnSelector implements DoubleColumnSelector
  {
    private final IncrementalIndexRowHolder currEntry;
    private final int metricIndex;

    public DoubleMetricColumnSelector(IncrementalIndexRowHolder currEntry, int metricIndex)
    {
      this.currEntry = currEntry;
      this.metricIndex = metricIndex;
    }

    @Override
    public double getDouble()
    {
      assert NullHandling.replaceWithDefault() || !isNull();
      return AggsManager.this.incrementalIndex.getMetricDoubleValue(currEntry.get(), metricIndex);
    }

    @Override
    public boolean isNull()
    {
      return AggsManager.this.incrementalIndex.isNull(currEntry.get(), metricIndex);
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("index", AggsManager.this.incrementalIndex);
    }
  }

  public ColumnValueSelector<?> makeMetricColumnValueSelector(String metric, IncrementalIndexRowHolder currEntry)
  {
    MetricDesc metricDesc = metricDescs.get(metric);
    if (metricDesc == null) {
      return NilColumnValueSelector.instance();
    }
    int metricIndex = metricDesc.getIndex();
    switch (metricDesc.getCapabilities().getType()) {
      case COMPLEX:
        return new ObjectMetricColumnSelector(metricDesc, currEntry, metricIndex);
      case LONG:
        return new LongMetricColumnSelector(currEntry, metricIndex);
      case FLOAT:
        return new FloatMetricColumnSelector(currEntry, metricIndex);
      case DOUBLE:
        return new DoubleMetricColumnSelector(currEntry, metricIndex);
      case STRING:
        throw new IllegalStateException("String is not a metric column type");
      default:
        throw new ISE("Unknown metric value type: %s", metricDesc.getCapabilities().getType());
    }
  }

  public static final class MetricDesc
  {
    private final int index;
    private final String name;
    private final String type;
    private final ColumnCapabilitiesImpl capabilities;

    public MetricDesc(int index, AggregatorFactory factory)
    {
      this.index = index;
      this.name = factory.getName();

      String typeInfo = factory.getTypeName();
      this.capabilities = new ColumnCapabilitiesImpl();
      if (typeInfo.equalsIgnoreCase("float")) {
        capabilities.setType(ValueType.FLOAT);
        this.type = typeInfo;
      } else if (typeInfo.equalsIgnoreCase("long")) {
        capabilities.setType(ValueType.LONG);
        this.type = typeInfo;
      } else if (typeInfo.equalsIgnoreCase("double")) {
        capabilities.setType(ValueType.DOUBLE);
        this.type = typeInfo;
      } else {
        capabilities.setType(ValueType.COMPLEX);
        this.type = ComplexMetrics.getSerdeForType(typeInfo).getTypeName();
      }
    }

    public int getIndex()
    {
      return index;
    }

    public String getName()
    {
      return name;
    }

    public String getType()
    {
      return type;
    }

    public ColumnCapabilitiesImpl getCapabilities()
    {
      return capabilities;
    }
  }

}
