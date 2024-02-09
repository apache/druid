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

import {
  Button,
  ButtonGroup,
  FormGroup,
  InputGroup,
  Intent,
  NumericInput,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import type { JSX } from 'react';
import React from 'react';

import { deepDelete, deepGet, deepSet, durationSanitizer } from '../../utils';
import { ArrayInput } from '../array-input/array-input';
import { FancyNumericInput } from '../fancy-numeric-input/fancy-numeric-input';
import { FormGroupWithInfo } from '../form-group-with-info/form-group-with-info';
import { IntervalInput } from '../interval-input/interval-input';
import { JsonInput } from '../json-input/json-input';
import { PopoverText } from '../popover-text/popover-text';
import { SuggestibleInput } from '../suggestible-input/suggestible-input';
import type { Suggestion } from '../suggestion-menu/suggestion-menu';

import './auto-form.scss';

export type Functor<M, R> = R | ((model: Partial<M>) => R);

export interface Field<M> {
  name: string;
  label?: string;
  info?: React.ReactNode;
  type:
    | 'number'
    | 'ratio'
    | 'size-bytes'
    | 'string'
    | 'duration'
    | 'boolean'
    | 'string-array'
    | 'json'
    | 'interval'
    | 'custom';
  defaultValue?: any;
  emptyValue?: any;
  suggestions?: Functor<M, Suggestion[]>;
  placeholder?: Functor<M, string>;
  min?: number;
  max?: number;
  zeroMeansUndefined?: boolean;
  height?: string;
  disabled?: Functor<M, boolean>;
  defined?: Functor<M, boolean | undefined>;
  required?: Functor<M, boolean>;
  multiline?: Functor<M, boolean>;
  hide?: Functor<M, boolean>;
  hideInMore?: Functor<M, boolean>;
  valueAdjustment?: (value: any) => any;
  adjustment?: (model: Partial<M>, oldModel: Partial<M>) => Partial<M>;
  issueWithValue?: (value: any) => string | undefined;

  customSummary?: (v: any) => string;
  customDialog?: (o: {
    value: any;
    onValueChange: (v: any) => void;
    onClose: () => void;
  }) => JSX.Element;
}

function toNumberOrUndefined(n: unknown): number | undefined {
  if (n == null) return;
  const r = Number(n);
  return isNaN(r) ? undefined : r;
}

interface ComputedFieldValues {
  required: boolean;
  defaultValue?: any;
  modelValue: any;
}

export interface AutoFormProps<M> {
  fields: Field<M>[];
  model: Partial<M> | undefined;
  onChange: (newModel: Partial<M>) => void;
  onFinalize?: () => void;
  showCustom?: (model: Partial<M>) => boolean;
  large?: boolean;
  globalAdjustment?: (model: Partial<M>) => Partial<M>;
}

export interface AutoFormState {
  showMore: boolean;
  customDialog?: JSX.Element;
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

  static computeFieldValues<M>(model: M | undefined, field: Field<M>): ComputedFieldValues {
    const required = AutoForm.evaluateFunctor(field.required, model, false);
    return {
      required,
      defaultValue: required ? undefined : field.defaultValue,
      modelValue: deepGet(model as any, field.name),
    };
  }

  static evaluateFunctor<M, R>(
    functor: undefined | Functor<M, R>,
    model: Partial<M> | undefined,
    defaultValue: R,
  ): R {
    if (!model || functor == null) return defaultValue;
    if (typeof functor === 'function') {
      return (functor as any)(model);
    } else {
      return functor;
    }
  }

  static isValidModel<M>(model: Partial<M> | undefined, fields: readonly Field<M>[]): model is M {
    return !AutoForm.issueWithModel(model, fields);
  }

  static issueWithModel<M>(
    model: Partial<M> | undefined,
    fields: readonly Field<M>[],
  ): string | undefined {
    if (typeof model === 'undefined') {
      return `model is undefined`;
    }

    // Precompute which fields are defined because fields could be defined twice and only one should do the checking
    const definedFields: Record<string, Field<M>> = {};
    const notDefinedFields: Record<string, Field<M>> = {};
    for (const field of fields) {
      const fieldDefined = AutoForm.evaluateFunctor(field.defined, model, true);
      if (fieldDefined) {
        definedFields[field.name] = field;
      } else if (fieldDefined === false) {
        notDefinedFields[field.name] = field;
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
      } else if (notDefinedFields[field.name]) {
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

  private readonly fieldChange = (field: Field<T>, newValue: any) => {
    const { model } = this.props;
    if (!model) return;

    if (field.valueAdjustment) {
      newValue = field.valueAdjustment(newValue);
    }

    let newModel: Partial<T>;
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
      newModel = field.adjustment(newModel, model);
    }

    this.modelChange(newModel);
  };

  private readonly modelChange = (newModel: Partial<T>) => {
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
    const { required, defaultValue, modelValue } = AutoForm.computeFieldValues(model, field);

    return (
      <FancyNumericInput
        value={toNumberOrUndefined(modelValue)}
        defaultValue={toNumberOrUndefined(defaultValue)}
        onValueChange={valueAsNumber => {
          this.fieldChange(
            field,
            valueAsNumber === 0 && field.zeroMeansUndefined ? undefined : valueAsNumber,
          );
        }}
        onBlur={e => {
          if (e.target.value === '') {
            this.fieldChange(field, undefined);
          }
          if (onFinalize) onFinalize();
        }}
        min={field.min ?? 0}
        max={field.max}
        fill
        large={large}
        disabled={AutoForm.evaluateFunctor(field.disabled, model, false)}
        placeholder={AutoForm.evaluateFunctor(field.placeholder, model, '')}
        intent={required && modelValue == null ? AutoForm.REQUIRED_INTENT : undefined}
      />
    );
  }

  private renderRatioInput(field: Field<T>): JSX.Element {
    const { model, large, onFinalize } = this.props;
    const { required, defaultValue, modelValue } = AutoForm.computeFieldValues(model, field);

    return (
      <FancyNumericInput
        value={toNumberOrUndefined(modelValue)}
        defaultValue={toNumberOrUndefined(defaultValue)}
        onValueChange={valueAsNumber => {
          this.fieldChange(
            field,
            valueAsNumber === 0 && field.zeroMeansUndefined ? undefined : valueAsNumber,
          );
        }}
        onBlur={e => {
          if (e.target.value === '') {
            this.fieldChange(field, undefined);
          }
          if (onFinalize) onFinalize();
        }}
        min={field.min ?? 0}
        max={field.max ?? 1}
        minorStepSize={0.001}
        stepSize={0.01}
        majorStepSize={0.05}
        fill
        large={large}
        disabled={AutoForm.evaluateFunctor(field.disabled, model, false)}
        placeholder={AutoForm.evaluateFunctor(field.placeholder, model, '')}
        intent={required && modelValue == null ? AutoForm.REQUIRED_INTENT : undefined}
      />
    );
  }

  private renderSizeBytesInput(field: Field<T>): JSX.Element {
    const { model, large, onFinalize } = this.props;
    const { required, defaultValue, modelValue } = AutoForm.computeFieldValues(model, field);

    return (
      <NumericInput
        value={modelValue || defaultValue}
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
        intent={required && modelValue == null ? AutoForm.REQUIRED_INTENT : undefined}
      />
    );
  }

  private renderStringInput(field: Field<T>, sanitizer?: (str: string) => string): JSX.Element {
    const { model, large, onFinalize } = this.props;
    const { required, defaultValue, modelValue } = AutoForm.computeFieldValues(model, field);

    return (
      <SuggestibleInput
        value={modelValue != null ? modelValue : defaultValue || ''}
        sanitizer={sanitizer}
        issueWithValue={field.issueWithValue}
        onValueChange={v => {
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
        intent={required && modelValue == null ? AutoForm.REQUIRED_INTENT : undefined}
        multiline={AutoForm.evaluateFunctor(field.multiline, model, false)}
        height={field.height}
      />
    );
  }

  private renderBooleanInput(field: Field<T>): JSX.Element {
    const { model, large, onFinalize } = this.props;
    const { required, defaultValue, modelValue } = AutoForm.computeFieldValues(model, field);
    const shownValue = modelValue == null ? defaultValue : modelValue;
    const disabled = AutoForm.evaluateFunctor(field.disabled, model, false);
    const intent = required && modelValue == null ? AutoForm.REQUIRED_INTENT : undefined;

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
    const { required, defaultValue, modelValue } = AutoForm.computeFieldValues(model, field);

    return (
      <ArrayInput
        values={modelValue || defaultValue || []}
        onChange={(v: any) => {
          this.fieldChange(field, v);
        }}
        placeholder={AutoForm.evaluateFunctor(field.placeholder, model, '')}
        large={large}
        disabled={AutoForm.evaluateFunctor(field.disabled, model, false)}
        intent={required && modelValue == null ? AutoForm.REQUIRED_INTENT : undefined}
        suggestions={AutoForm.evaluateFunctor(field.suggestions, model, undefined)}
      />
    );
  }

  private renderIntervalInput(field: Field<T>): JSX.Element {
    const { model } = this.props;
    const { required, defaultValue, modelValue } = AutoForm.computeFieldValues(model, field);

    return (
      <IntervalInput
        interval={modelValue != null ? modelValue : defaultValue || ''}
        onValueChange={(v: any) => {
          this.fieldChange(field, v);
        }}
        placeholder={AutoForm.evaluateFunctor(field.placeholder, model, '')}
        intent={required && modelValue == null ? AutoForm.REQUIRED_INTENT : undefined}
      />
    );
  }

  private renderCustomInput(field: Field<T>): JSX.Element {
    const { model } = this.props;
    const { required, defaultValue, modelValue } = AutoForm.computeFieldValues(model, field);
    const effectiveValue = modelValue || defaultValue;

    const onEdit = () => {
      this.setState({
        customDialog: field.customDialog?.({
          value: effectiveValue,
          onValueChange: v => this.fieldChange(field, v),
          onClose: () => {
            this.setState({ customDialog: undefined });
          },
        }),
      });
    };

    return (
      <InputGroup
        className="custom-input"
        value={(field.customSummary || String)(effectiveValue)}
        intent={required && modelValue == null ? AutoForm.REQUIRED_INTENT : undefined}
        readOnly
        placeholder={AutoForm.evaluateFunctor(field.placeholder, model, '')}
        rightElement={<Button icon={IconNames.EDIT} minimal onClick={onEdit} />}
        onClick={onEdit}
      />
    );
  }

  renderFieldInput(field: Field<T>) {
    switch (field.type) {
      case 'number':
        return this.renderNumberInput(field);
      case 'ratio':
        return this.renderRatioInput(field);
      case 'size-bytes':
        return this.renderSizeBytesInput(field);
      case 'string':
        return this.renderStringInput(field);
      case 'duration':
        return this.renderStringInput(field, durationSanitizer);
      case 'boolean':
        return this.renderBooleanInput(field);
      case 'string-array':
        return this.renderStringArrayInput(field);
      case 'json':
        return this.renderJsonInput(field);
      case 'interval':
        return this.renderIntervalInput(field);
      case 'custom':
        return this.renderCustomInput(field);
      default:
        throw new Error(`unknown field type '${field.type}'`);
    }
  }

  private readonly renderField = (field: Field<T>) => {
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

  render() {
    const { fields, model, showCustom } = this.props;
    const { showMore, customDialog } = this.state;

    let shouldShowMore = false;
    const shownFields = fields.filter(field => {
      if (AutoForm.evaluateFunctor(field.defined, model, true)) {
        if (AutoForm.evaluateFunctor(field.hide, model, false)) {
          return false;
        }

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
        {customDialog}
      </div>
    );
  }
}
