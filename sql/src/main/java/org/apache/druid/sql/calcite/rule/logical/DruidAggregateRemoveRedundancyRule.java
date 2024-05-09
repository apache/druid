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

package org.apache.druid.sql.calcite.rule.logical;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Generated;
import org.immutables.value.Value;

import javax.annotation.CheckReturnValue;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Planner rule that recognizes a {@link Aggregate}
 * on top of a {@link Project} and if possible
 * aggregate through the project or removes the project.
 *
 * This is updated version of {@link org.apache.calcite.rel.rules.AggregateProjectMergeRule}
 * to be able to handle expressions.
 */
@Value.Enclosing
public class DruidAggregateRemoveRedundancyRule
    extends RelRule<DruidAggregateRemoveRedundancyRule.Config>
    implements TransformationRule
{

  /**
   * Creates a DruidAggregateRemoveRedundancyRule.
   */
  protected DruidAggregateRemoveRedundancyRule(Config config)
  {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Aggregate aggregate = call.rel(0);
    final Project project = call.rel(1);
    RelNode x = apply(call, aggregate, project);
    if (x != null) {
      call.transformTo(x);
      call.getPlanner().prune(aggregate);
    }
  }

  public static @Nullable RelNode apply(RelOptRuleCall call, Aggregate aggregate, Project project)
  {
    final Set<Integer> interestingFields = RelOptUtil.getAllFields(aggregate);
    if (interestingFields.isEmpty()) {
      return null;
    }
    final Map<Integer, Integer> map = new HashMap<>();
    final Map<RexNode, Integer> assignedRefForExpr = new HashMap<>();
    List<RexNode> newRexNodes = new ArrayList<>();
    for (int source : interestingFields) {
      final RexNode rex = project.getProjects().get(source);
      if (!assignedRefForExpr.containsKey(rex)) {
        RexNode newNode = new RexInputRef(source, rex.getType());
        assignedRefForExpr.put(rex, newRexNodes.size());
        newRexNodes.add(newNode);
      }
      map.put(source, assignedRefForExpr.get(rex));
    }

    if (newRexNodes.size() == project.getProjects().size()) {
      return null;
    }

    final ImmutableBitSet newGroupSet = aggregate.getGroupSet().permute(map);
    ImmutableList<ImmutableBitSet> newGroupingSets = null;
    if (aggregate.getGroupType() != Group.SIMPLE) {
      newGroupingSets =
          ImmutableBitSet.ORDERING.immutableSortedCopy(
              Sets.newTreeSet(ImmutableBitSet.permute(aggregate.getGroupSets(), map)));
    }

    final ImmutableList.Builder<AggregateCall> aggCalls = ImmutableList.builder();
    final int sourceCount = aggregate.getInput().getRowType().getFieldCount();
    final int targetCount = newRexNodes.size();
    final Mappings.TargetMapping targetMapping = Mappings.target(map, sourceCount, targetCount);
    for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
      aggCalls.add(aggregateCall.transform(targetMapping));
    }

    final RelBuilder relBuilder = call.builder();
    relBuilder.push(project);
    relBuilder.project(newRexNodes);

    final Aggregate newAggregate =
        aggregate.copy(aggregate.getTraitSet(), relBuilder.build(),
                       newGroupSet, newGroupingSets, aggCalls.build()
        );
    relBuilder.push(newAggregate);

    final List<Integer> newKeys =
        Util.transform(
            aggregate.getGroupSet().asList(),
            key -> Objects.requireNonNull(
                map.get(key),
                () -> "no value found for key " + key + " in " + map
            )
        );

    // Add a project if the group set is not in the same order or
    // contains duplicates.
    if (!newKeys.equals(newGroupSet.asList())) {
      final List<Integer> posList = new ArrayList<>();
      for (int newKey : newKeys) {
        posList.add(newGroupSet.indexOf(newKey));
      }
      for (int i = newAggregate.getGroupCount();
           i < newAggregate.getRowType().getFieldCount(); i++) {
        posList.add(i);
      }
      relBuilder.project(relBuilder.fields(posList));
    }

    return relBuilder.build();
  }

  /**
   * Rule configuration.
   */
  @Value.Immutable
  public interface Config extends RelRule.Config
  {
    Config DEFAULT = DruidImmutableAggregateRemoveRedundancyRule.Config.of()
                                                                       .withOperandFor(Aggregate.class, Project.class);

    @Override
    default DruidAggregateRemoveRedundancyRule toRule()
    {
      return new DruidAggregateRemoveRedundancyRule(this);
    }

    /**
     * Defines an operand tree for the given classes.
     */
    default Config withOperandFor(
        Class<? extends Aggregate> aggregateClass,
        Class<? extends Project> projectClass
    )
    {
      return withOperandSupplier(b0 ->
                                     b0.operand(aggregateClass).oneInput(b1 ->
                                                                             b1.operand(projectClass).anyInputs())).as(
          Config.class);
    }
  }

  /**
   * Custom version of {@link org.apache.calcite.rel.rules.ImmutableAggregateProjectMergeRule}
   */
  @ParametersAreNonnullByDefault
  @Generated(
      from = "DruidAggregateRemoveRedundancyRule",
      generator = "Immutables"
  )
  static final class DruidImmutableAggregateRemoveRedundancyRule
  {
    private DruidImmutableAggregateRemoveRedundancyRule()
    {
    }

    @CheckReturnValue
    @Generated(
        from = "DruidAggregateRemoveRedundancyRule.Config",
        generator = "Immutables"
    )
    @Immutable
    static final class Config implements DruidAggregateRemoveRedundancyRule.Config
    {
      private final RelBuilderFactory relBuilderFactory;
      @javax.annotation.Nullable
      private final @Nullable String description;
      private final OperandTransform operandSupplier;
      private static final byte STAGE_INITIALIZING = -1;
      private static final byte STAGE_UNINITIALIZED = 0;
      private static final byte STAGE_INITIALIZED = 1;
      private transient volatile InitShim initShim;
      private static final Config INSTANCE = validate(new Config());

      private Config()
      {
        this.initShim = new InitShim();
        this.description = null;
        this.relBuilderFactory = this.initShim.relBuilderFactory();
        this.operandSupplier = this.initShim.operandSupplier();
        this.initShim = null;
      }

      private Config(Builder builder)
      {
        this.initShim = new InitShim();
        this.description = builder.description;
        if (builder.relBuilderFactory != null) {
          this.initShim.withRelBuilderFactory(builder.relBuilderFactory);
        }

        if (builder.operandSupplier != null) {
          this.initShim.withOperandSupplier(builder.operandSupplier);
        }

        this.relBuilderFactory = this.initShim.relBuilderFactory();
        this.operandSupplier = this.initShim.operandSupplier();
        this.initShim = null;
      }

      private Config(
          RelBuilderFactory relBuilderFactory,
          @javax.annotation.Nullable @Nullable String description,
          OperandTransform operandSupplier
      )
      {
        this.initShim = new InitShim();
        this.relBuilderFactory = relBuilderFactory;
        this.description = description;
        this.operandSupplier = operandSupplier;
        this.initShim = null;
      }

      private RelBuilderFactory relBuilderFactoryInitialize()
      {
        return RelFactories.LOGICAL_BUILDER;
      }

      private OperandTransform operandSupplierInitialize()
      {
        return s -> {
          throw new IllegalArgumentException("Rules must have at least one "
                                             + "operand. Call Config.withOperandSupplier to specify them.");
        };
      }

      @Override
      public RelBuilderFactory relBuilderFactory()
      {
        InitShim shim = this.initShim;
        return shim != null ? shim.relBuilderFactory() : this.relBuilderFactory;
      }

      @Override
      @javax.annotation.Nullable
      public @Nullable String description()
      {
        return this.description;
      }

      @Override
      public OperandTransform operandSupplier()
      {
        InitShim shim = this.initShim;
        return shim != null ? shim.operandSupplier() : this.operandSupplier;
      }

      @Override
      public Config withRelBuilderFactory(RelBuilderFactory value)
      {
        if (this.relBuilderFactory == value) {
          return this;
        } else {
          RelBuilderFactory newValue = (RelBuilderFactory) Objects.requireNonNull(value, "relBuilderFactory");
          return validate(new Config(newValue, this.description, this.operandSupplier));
        }
      }

      @Override
      public Config withDescription(@javax.annotation.Nullable @Nullable String value)
      {
        return Objects.equals(this.description, value)
               ? this
               : validate(new Config(this.relBuilderFactory, value, this.operandSupplier));
      }

      @Override
      public Config withOperandSupplier(OperandTransform value)
      {
        if (this.operandSupplier == value) {
          return this;
        } else {
          OperandTransform newValue = (OperandTransform) Objects.requireNonNull(value, "operandSupplier");
          return validate(new Config(this.relBuilderFactory, this.description, newValue));
        }
      }

      @Override
      public boolean equals(@javax.annotation.Nullable Object another)
      {
        if (this == another) {
          return true;
        } else {
          return another instanceof Config && this.equalTo((Config) another);
        }
      }

      private boolean equalTo(Config another)
      {
        return this.relBuilderFactory.equals(another.relBuilderFactory) && Objects.equals(
            this.description,
            another.description
        ) && this.operandSupplier.equals(another.operandSupplier);
      }

      @Override
      public int hashCode()
      {
        int h = 5381;
        h += (h << 5) + this.relBuilderFactory.hashCode();
        h += (h << 5) + Objects.hashCode(this.description);
        h += (h << 5) + this.operandSupplier.hashCode();
        return h;
      }

      @Override
      public String toString()
      {
        return MoreObjects.toStringHelper("Config")
                          .omitNullValues()
                          .add("relBuilderFactory", this.relBuilderFactory)
                          .add("description", this.description)
                          .add("operandSupplier", this.operandSupplier)
                          .toString();
      }

      public static Config of()
      {
        return INSTANCE;
      }

      private static Config validate(Config instance)
      {
        return INSTANCE != null && INSTANCE.equalTo(instance) ? INSTANCE : instance;
      }

      public static Config copyOf(DruidAggregateRemoveRedundancyRule.Config instance)
      {
        return instance instanceof Config ? (Config) instance : builder().from(instance).build();
      }

      public static Builder builder()
      {
        return new Builder();
      }

      @Generated(
          from = "DruidAggregateRemoveRedundancyRule.Config",
          generator = "Immutables"
      )
      @NotThreadSafe
      public static final class Builder
      {
        @javax.annotation.Nullable
        private RelBuilderFactory relBuilderFactory;
        @javax.annotation.Nullable
        private @Nullable String description;
        @javax.annotation.Nullable
        private OperandTransform operandSupplier;

        private Builder()
        {
        }

        @CanIgnoreReturnValue
        public Builder from(RelRule.Config instance)
        {
          Objects.requireNonNull(instance, "instance");
          this.from((Object) instance);
          return this;
        }

        @CanIgnoreReturnValue
        public Builder from(DruidAggregateRemoveRedundancyRule.Config instance)
        {
          Objects.requireNonNull(instance, "instance");
          this.from((Object) instance);
          return this;
        }

        private void from(Object object)
        {
          if (object instanceof RelRule.Config) {
            RelRule.Config instance = (RelRule.Config) object;
            this.withRelBuilderFactory(instance.relBuilderFactory());
            this.withOperandSupplier(instance.operandSupplier());
            String descriptionValue = instance.description();
            if (descriptionValue != null) {
              this.withDescription(descriptionValue);
            }
          }

        }

        @CanIgnoreReturnValue
        public Builder withRelBuilderFactory(RelBuilderFactory relBuilderFactory)
        {
          this.relBuilderFactory = (RelBuilderFactory) Objects.requireNonNull(relBuilderFactory, "relBuilderFactory");
          return this;
        }

        @CanIgnoreReturnValue
        public Builder withDescription(
            @javax.annotation.Nullable @Nullable String description
        )
        {
          this.description = description;
          return this;
        }

        @CanIgnoreReturnValue
        public Builder withOperandSupplier(OperandTransform operandSupplier)
        {
          this.operandSupplier = (OperandTransform) Objects.requireNonNull(operandSupplier, "operandSupplier");
          return this;
        }

        public Config build()
        {
          return Config.validate(new Config(this));
        }
      }

      @Generated(
          from = "DruidAggregateRemoveRedundancyRule.Config",
          generator = "Immutables"
      )
      private final class InitShim
      {
        private byte relBuilderFactoryBuildStage;
        private RelBuilderFactory relBuilderFactory;
        private byte operandSupplierBuildStage;
        private OperandTransform operandSupplier;

        private InitShim()
        {
          this.relBuilderFactoryBuildStage = 0;
          this.operandSupplierBuildStage = 0;
        }

        RelBuilderFactory relBuilderFactory()
        {
          if (this.relBuilderFactoryBuildStage == -1) {
            throw new IllegalStateException(this.formatInitCycleMessage());
          } else {
            if (this.relBuilderFactoryBuildStage == 0) {
              this.relBuilderFactoryBuildStage = -1;
              this.relBuilderFactory = (RelBuilderFactory) Objects.requireNonNull(
                  Config.this.relBuilderFactoryInitialize(),
                  "relBuilderFactory"
              );
              this.relBuilderFactoryBuildStage = 1;
            }

            return this.relBuilderFactory;
          }
        }

        void withRelBuilderFactory(RelBuilderFactory relBuilderFactory)
        {
          this.relBuilderFactory = relBuilderFactory;
          this.relBuilderFactoryBuildStage = 1;
        }

        OperandTransform operandSupplier()
        {
          if (this.operandSupplierBuildStage == -1) {
            throw new IllegalStateException(this.formatInitCycleMessage());
          } else {
            if (this.operandSupplierBuildStage == 0) {
              this.operandSupplierBuildStage = -1;
              this.operandSupplier = (OperandTransform) Objects.requireNonNull(
                  Config.this.operandSupplierInitialize(),
                  "operandSupplier"
              );
              this.operandSupplierBuildStage = 1;
            }

            return this.operandSupplier;
          }
        }

        void withOperandSupplier(OperandTransform operandSupplier)
        {
          this.operandSupplier = operandSupplier;
          this.operandSupplierBuildStage = 1;
        }

        private String formatInitCycleMessage()
        {
          List<String> attributes = new ArrayList();
          if (this.relBuilderFactoryBuildStage == -1) {
            attributes.add("relBuilderFactory");
          }

          if (this.operandSupplierBuildStage == -1) {
            attributes.add("operandSupplier");
          }

          return "Cannot build Config, attribute initializers form cycle " + attributes;
        }
      }
    }
  }
}
