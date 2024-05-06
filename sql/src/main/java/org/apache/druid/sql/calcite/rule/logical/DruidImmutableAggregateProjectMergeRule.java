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
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;
import org.immutables.value.Generated;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Custom version of {@link org.apache.calcite.rel.rules.ImmutableAggregateProjectMergeRule}
 */
@ParametersAreNonnullByDefault
@Generated(
    from = "DruidAggregateProjectMergeRule",
    generator = "Immutables"
)
final class DruidImmutableAggregateProjectMergeRule
{
  private DruidImmutableAggregateProjectMergeRule()
  {
  }

  @CheckReturnValue
  @Generated(
      from = "DruidAggregateProjectMergeRule.Config",
      generator = "Immutables"
  )
  @Immutable
  static final class Config implements DruidAggregateProjectMergeRule.Config
  {
    private final RelBuilderFactory relBuilderFactory;
    @Nullable
    private final @org.checkerframework.checker.nullness.qual.Nullable String description;
    private final RelRule.OperandTransform operandSupplier;
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
        @Nullable @org.checkerframework.checker.nullness.qual.Nullable String description,
        RelRule.OperandTransform operandSupplier
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

    private RelRule.OperandTransform operandSupplierInitialize()
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
    @Nullable
    public @org.checkerframework.checker.nullness.qual.Nullable String description()
    {
      return this.description;
    }

    @Override
    public RelRule.OperandTransform operandSupplier()
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
    public Config withDescription(@Nullable @org.checkerframework.checker.nullness.qual.Nullable String value)
    {
      return Objects.equals(this.description, value)
             ? this
             : validate(new Config(this.relBuilderFactory, value, this.operandSupplier));
    }

    @Override
    public Config withOperandSupplier(RelRule.OperandTransform value)
    {
      if (this.operandSupplier == value) {
        return this;
      } else {
        RelRule.OperandTransform newValue = (RelRule.OperandTransform) Objects.requireNonNull(value, "operandSupplier");
        return validate(new Config(this.relBuilderFactory, this.description, newValue));
      }
    }

    @Override
    public boolean equals(@Nullable Object another)
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

    public static Config copyOf(DruidAggregateProjectMergeRule.Config instance)
    {
      return instance instanceof Config ? (Config) instance : builder().from(instance).build();
    }

    public static Builder builder()
    {
      return new Builder();
    }

    @Generated(
        from = "DruidAggregateProjectMergeRule.Config",
        generator = "Immutables"
    )
    @NotThreadSafe
    public static final class Builder
    {
      @Nullable
      private RelBuilderFactory relBuilderFactory;
      @Nullable
      private @org.checkerframework.checker.nullness.qual.Nullable String description;
      @Nullable
      private RelRule.OperandTransform operandSupplier;

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
      public Builder from(DruidAggregateProjectMergeRule.Config instance)
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
          @Nullable @org.checkerframework.checker.nullness.qual.Nullable String description
      )
      {
        this.description = description;
        return this;
      }

      @CanIgnoreReturnValue
      public Builder withOperandSupplier(RelRule.OperandTransform operandSupplier)
      {
        this.operandSupplier = (RelRule.OperandTransform) Objects.requireNonNull(operandSupplier, "operandSupplier");
        return this;
      }

      public Config build()
      {
        return DruidImmutableAggregateProjectMergeRule.Config.validate(new Config(this));
      }
    }

    @Generated(
        from = "DruidAggregateProjectMergeRule.Config",
        generator = "Immutables"
    )
    private final class InitShim
    {
      private byte relBuilderFactoryBuildStage;
      private RelBuilderFactory relBuilderFactory;
      private byte operandSupplierBuildStage;
      private RelRule.OperandTransform operandSupplier;

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

      RelRule.OperandTransform operandSupplier()
      {
        if (this.operandSupplierBuildStage == -1) {
          throw new IllegalStateException(this.formatInitCycleMessage());
        } else {
          if (this.operandSupplierBuildStage == 0) {
            this.operandSupplierBuildStage = -1;
            this.operandSupplier = (RelRule.OperandTransform) Objects.requireNonNull(
                Config.this.operandSupplierInitialize(),
                "operandSupplier"
            );
            this.operandSupplierBuildStage = 1;
          }

          return this.operandSupplier;
        }
      }

      void withOperandSupplier(RelRule.OperandTransform operandSupplier)
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
