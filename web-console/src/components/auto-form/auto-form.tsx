/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Button, ButtonGroup, FormGroup, Intent, NumericInput } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';

import { deepDelete, deepGet, deepSet } from '../../utils';
import { ArrayInput } from '../array-input/array-input';
import { FormGroupWithInfo } from '../form-group-with-info/form-group-with-info';
import { IntervalInput } from '../interval-input/interval-input';
import { JsonInput } from '../json-input/json-input';
import { NumericInputWithDefault } from '../numeric-input-with-default/numeric-input-with-default';
import { PopoverText } from '../popover-text/popover-text';
import { SuggestibleInput, Suggestion } from '../suggestible-input/suggestible-input';

import './auto-form.scss';

export type Functor<M, R> = R | ((model: M) => R);

export interface Field<M> {
  name: string;
  label?: string;
  info?: React.ReactNode;
  type:
    | 'number'
    | 'size-bytes'
    | 'string'
    | 'duration'
    | 'boolean'
    | 'string-array'
    | 'json'
    | 'interval';
  defaultValue?: any;
  emptyValue?: any;
  suggestions?: Functor<M, Suggestion[]>;
  placeholder?: Functor<M, string>;
  min?: number;
  zeroMeansUndefined?: boolean;
  height?: string;
  disabled?: Functor<M, boolean>;
  defined?: Functor<M, boolean>;
  required?: Functor<M, boolean>;
  hideInMore?: Functor<M, boolean>;
  valueAdjustment?: (value: any) => any;
  adjustment?: (model: M) => M;
  issueWithValue?: (value: any) => string | undefined;
}

export interface AutoFormProps<M> {
  fields: Field<M>[];
  model: M | undefined;
  onChange: (newModel: M) => void;
  onFinalize?: () => void;
  showCustom?: (model: M) => boolean;
  large?: boolean;
  globalAdjustment?: (model: M) => M;
}

export interface AutoFormState {
  showMore: boolean;
}

export class AutoForm<T extends Record<string, any>> extends React.PureComponent<
  AutoFormProps<T>,
  AutoFormState
> {
  static REQUIRED_INTENT = Intent.PRIMARY;

  static makeLabelName(label: string): string {
    const parts = label.split('.');
    let newLabel = parts[parts.length - 1]
      .split(/(?=[A-Z])/)
      .join(' ')
      .toLowerCase()
      .replace(/\./g, ' ');
    newLabel = newLabel[0].toUpperCase() + newLabel.slice(1);
    return newLabel;
  }

  static evaluateFunctor<M, R>(
    functor: undefined | Functor<M, R>,
    model: M | undefined,
    defaultValue: R,
  ): R {
    if (!model || functor == null) return defaultValue;
    if (typeof functor === 'function') {
      return (functor as any)(model);
    } else {
      return functor;
    }
  }

  static issueWithModel<M>(model: M | undefined, fields: readonly Field<M>[]): string | undefined {
    if (typeof model === 'undefined') {
      return `model is undefined`;
    }

    // Precompute which fields are defined because fields could be defined twice and only one should do the checking
    const definedFields: Record<string, Field<M>> = {};
    for (const field of fields) {
      const fieldDefined = AutoForm.evaluateFunctor(field.defined, model, true);
      if (fieldDefined) {
        definedFields[field.name] = field;
      }
    }

    for (const field of fields) {
      const fieldValue = deepGet(model, field.name);
      const fieldValueDefined = fieldValue != null;
      const fieldThatIsDefined = definedFields[field.name];
      if (fieldThatIsDefined) {
        if (fieldThatIsDefined === field) {
          const fieldRequired = AutoForm.evaluateFunctor(field.required, model, false);
          if (fieldRequired) {
            if (!fieldValueDefined) {
              return `field ${field.name} is required`;
            }
          }

          if (fieldValueDefined && field.issueWithValue) {
            const valueIssue = field.issueWithValue(fieldValue);
            if (valueIssue) return `field ${field.name} has issue ${valueIssue}`;
          }
        }
      } else {
        // The field is undefined
        if (fieldValueDefined) {
          return `field ${field.name} is defined but it should not be`;
        }
      }
    }
    return;
  }

  constructor(props: AutoFormProps<T>) {
    super(props);
    this.state = {
      showMore: false,
    };
  }

  private fieldChange = (field: Field<T>, newValue: any) => {
    const { model } = this.props;
    if (!model) return;

    if (field.valueAdjustment) {
      newValue = field.valueAdjustment(newValue);
    }

    let newModel: T;
    if (typeof newValue === 'undefined') {
      if (typeof field.emptyValue === 'undefined') {
        newModel = deepDelete(model, field.name);
      } else {
        newModel = deepSet(model, field.name, field.emptyValue);
      }
    } else {
      newModel = deepSet(model, field.name, newValue);
    }

    if (field.adjustment) {
      newModel = field.adjustment(newModel);
    }

    this.modelChange(newModel);
  };

  private modelChange = (newModel: T) => {
    const { globalAdjustment, fields, onChange, model } = this.props;

    // Delete things that are not defined now (but were defined prior to the change)
    for (const someField of fields) {
      if (
        !AutoForm.evaluateFunctor(someField.defined, newModel, true) &&
        AutoForm.evaluateFunctor(someField.defined, model, true)
      ) {
        newModel = deepDelete(newModel, someField.name);
      }
    }

    // Perform any global adjustments if needed
    if (globalAdjustment) {
      newModel = globalAdjustment(newModel);
    }

    onChange(newModel);
  };

  private renderNumberInput(field: Field<T>): JSX.Element {
    const { model, large, onFinalize } = this.props;

    const modelValue = deepGet(model as any, field.name);
    return (
      <NumericInputWithDefault
        value={modelValue}
        defaultValue={field.defaultValue}
        onValueChange={(valueAsNumber: number, valueAsString: string) => {
          let newValue: number | undefined;
          if (valueAsString !== '' && !isNaN(valueAsNumber)) {
            newValue = valueAsNumber === 0 && field.zeroMeansUndefined ? undefined : valueAsNumber;
          }
          this.fieldChange(field, newValue);
        }}
        onBlur={e => {
          if (e.target.value === '') {
            this.fieldChange(field, undefined);
          }
          if (onFinalize) onFinalize();
        }}
        min={field.min || 0}
        fill
        large={large}
        disabled={AutoForm.evaluateFunctor(field.disabled, model, false)}
        placeholder={AutoForm.evaluateFunctor(field.placeholder, model, '')}
        intent={
          AutoForm.evaluateFunctor(field.required, model, false) && modelValue == null
            ? AutoForm.REQUIRED_INTENT
            : undefined
        }
      />
    );
  }

  private renderSizeBytesInput(field: Field<T>): JSX.Element {
    const { model, large, onFinalize } = this.props;

    return (
      <NumericInput
        value={deepGet(model as any, field.name) || field.defaultValue}
        onValueChange={(v: number) => {
          if (isNaN(v)) return;
          this.fieldChange(field, v);
        }}
        onBlur={() => {
          if (onFinalize) onFinalize();
        }}
        min={0}
        stepSize={1000}
        majorStepSize={1000000}
        fill
        large={large}
        disabled={AutoForm.evaluateFunctor(field.disabled, model, false)}
      />
    );
  }

  private renderStringInput(field: Field<T>, sanitize?: (str: string) => string): JSX.Element {
    const { model, large, onFinalize } = this.props;

    const modelValue = deepGet(model as any, field.name);
    return (
      <SuggestibleInput
        value={modelValue != null ? modelValue : field.defaultValue || ''}
        onValueChange={v => {
          if (sanitize && typeof v === 'string') v = sanitize(v);
          this.fieldChange(field, v);
        }}
        onBlur={() => {
          if (modelValue === '') this.fieldChange(field, undefined);
        }}
        onFinalize={onFinalize}
        placeholder={AutoForm.evaluateFunctor(field.placeholder, model, '')}
        suggestions={AutoForm.evaluateFunctor(field.suggestions, model, undefined)}
        large={large}
        disabled={AutoForm.evaluateFunctor(field.disabled, model, false)}
        intent={
          AutoForm.evaluateFunctor(field.required, model, false) && modelValue == null
            ? AutoForm.REQUIRED_INTENT
            : undefined
        }
      />
    );
  }

  private renderBooleanInput(field: Field<T>): JSX.Element {
    const { model, large, onFinalize } = this.props;
    const modelValue = deepGet(model as any, field.name);
    const shownValue = modelValue == null ? field.defaultValue : modelValue;
    const disabled = AutoForm.evaluateFunctor(field.disabled, model, false);
    const intent =
      AutoForm.evaluateFunctor(field.required, model, false) && modelValue == null
        ? AutoForm.REQUIRED_INTENT
        : undefined;

    return (
      <ButtonGroup large={large}>
        <Button
          intent={intent}
          disabled={disabled}
          active={shownValue === false}
          onClick={() => {
            this.fieldChange(field, false);
            if (onFinalize) onFinalize();
          }}
        >
          False
        </Button>
        <Button
          intent={intent}
          disabled={disabled}
          active={shownValue === true}
          onClick={() => {
            this.fieldChange(field, true);
            if (onFinalize) onFinalize();
          }}
        >
          True
        </Button>
      </ButtonGroup>
    );
  }

  private renderJsonInput(field: Field<T>): JSX.Element {
    const { model } = this.props;

    return (
      <JsonInput
        value={deepGet(model as any, field.name)}
        onChange={(v: any) => this.fieldChange(field, v)}
        placeholder={AutoForm.evaluateFunctor(field.placeholder, model, '')}
        height={field.height}
        issueWithValue={field.issueWithValue}
      />
    );
  }

  private renderStringArrayInput(field: Field<T>): JSX.Element {
    const { model, large } = this.props;
    const modelValue = deepGet(model as any, field.name);
    return (
      <ArrayInput
        values={modelValue || []}
        onChange={(v: any) => {
          this.fieldChange(field, v);
        }}
        placeholder={AutoForm.evaluateFunctor(field.placeholder, model, '')}
        large={large}
        disabled={AutoForm.evaluateFunctor(field.disabled, model, false)}
        intent={
          AutoForm.evaluateFunctor(field.required, model, false) && modelValue == null
            ? AutoForm.REQUIRED_INTENT
            : undefined
        }
      />
    );
  }

  private renderIntervalInput(field: Field<T>): JSX.Element {
    const { model } = this.props;

    const modelValue = deepGet(model as any, field.name);
    return (
      <IntervalInput
        interval={modelValue != null ? modelValue : field.defaultValue || ''}
        onValueChange={(v: any) => {
          this.fieldChange(field, v);
        }}
        placeholder={AutoForm.evaluateFunctor(field.placeholder, model, '')}
        intent={
          AutoForm.evaluateFunctor(field.required, model, false) && modelValue == null
            ? AutoForm.REQUIRED_INTENT
            : undefined
        }
      />
    );
  }

  renderFieldInput(field: Field<T>) {
    switch (field.type) {
      case 'number':
        return this.renderNumberInput(field);
      case 'size-bytes':
        return this.renderSizeBytesInput(field);
      case 'string':
        return this.renderStringInput(field);
      case 'duration':
        return this.renderStringInput(field, (str: string) =>
          str.toUpperCase().replace(/[^0-9PYMDTHS.,]/g, ''),
        );
      case 'boolean':
        return this.renderBooleanInput(field);
      case 'string-array':
        return this.renderStringArrayInput(field);
      case 'json':
        return this.renderJsonInput(field);
      case 'interval':
        return this.renderIntervalInput(field);
      default:
        throw new Error(`unknown field type '${field.type}'`);
    }
  }

  private renderField = (field: Field<T>) => {
    const { model } = this.props;
    if (!model) return;

    const label = field.label || AutoForm.makeLabelName(field.name);
    return (
      <FormGroupWithInfo
        key={field.name}
        label={label}
        info={field.info ? <PopoverText>{field.info}</PopoverText> : undefined}
      >
        {this.renderFieldInput(field)}
      </FormGroupWithInfo>
    );
  };

  renderCustom() {
    const { model } = this.props;

    return (
      <FormGroup label="Custom" key="custom">
        <JsonInput value={model} onChange={this.modelChange} />
      </FormGroup>
    );
  }

  renderMoreOrLess() {
    const { showMore } = this.state;

    return (
      <FormGroup key="more-or-less">
        <Button
          text={showMore ? 'Show less' : 'Show more'}
          rightIcon={showMore ? IconNames.CHEVRON_UP : IconNames.CHEVRON_DOWN}
          minimal
          fill
          onClick={() => {
            this.setState(({ showMore }) => ({ showMore: !showMore }));
          }}
        />
      </FormGroup>
    );
  }

  render(): JSX.Element {
    const { fields, model, showCustom } = this.props;
    const { showMore } = this.state;

    let shouldShowMore = false;
    const shownFields = fields.filter(field => {
      if (AutoForm.evaluateFunctor(field.defined, model, true)) {
        if (AutoForm.evaluateFunctor(field.hideInMore, model, false)) {
          shouldShowMore = true;
          return showMore;
        }
        return true;
      } else {
        return false;
      }
    });

    return (
      <div className="auto-form">
        {model && shownFields.map(this.renderField)}
        {model && showCustom && showCustom(model) && this.renderCustom()}
        {shouldShowMore && this.renderMoreOrLess()}
      </div>
    );
  }
}
