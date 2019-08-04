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

import { FormGroup, HTMLSelect, Icon, NumericInput, Popover } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';

import { deepDelete, deepGet, deepSet } from '../../utils/object-change';
import { ArrayInput } from '../array-input/array-input';
import { JSONInput } from '../json-input/json-input';
import { SuggestibleInput, SuggestionGroup } from '../suggestible-input/suggestible-input';

import './auto-form.scss';

export interface Field<T> {
  name: string;
  label?: string;
  info?: React.ReactNode;
  type: 'number' | 'size-bytes' | 'string' | 'duration' | 'boolean' | 'string-array' | 'json';
  defaultValue?: any;
  isDefined?: (model: T) => boolean;
  disabled?: boolean;
  suggestions?: (string | SuggestionGroup)[];
  placeholder?: string;
  min?: number;
}

export interface AutoFormProps<T> {
  fields: Field<T>[];
  model: T | undefined;
  onChange: (newModel: T) => void;
  showCustom?: (model: T) => boolean;
  updateJSONValidity?: (jsonValidity: boolean) => void;
  large?: boolean;
}

export interface AutoFormState {
  jsonInputsValidity: any;
}

export class AutoForm<T extends Record<string, any>> extends React.PureComponent<
  AutoFormProps<T>,
  AutoFormState
> {
  static makeLabelName(label: string): string {
    let newLabel = label
      .split(/(?=[A-Z])/)
      .join(' ')
      .toLowerCase()
      .replace(/\./g, ' ');
    newLabel = newLabel[0].toUpperCase() + newLabel.slice(1);
    return newLabel;
  }

  constructor(props: AutoFormProps<T>) {
    super(props);
    this.state = {
      jsonInputsValidity: {},
    };
  }

  private fieldChange = (field: Field<T>, newValue: any) => {
    const { model } = this.props;
    if (!model) return;
    const newModel =
      typeof newValue === 'undefined'
        ? deepDelete(model, field.name)
        : deepSet(model, field.name, newValue);
    this.modelChange(newModel);
  };

  private modelChange = (newModel: T) => {
    const { fields, onChange } = this.props;

    for (const someField of fields) {
      if (someField.isDefined && !someField.isDefined(newModel)) {
        newModel = deepDelete(newModel, someField.name);
      } else if (
        typeof someField.defaultValue !== 'undefined' &&
        typeof deepGet(newModel, someField.name) === 'undefined'
      ) {
        newModel = deepSet(newModel, someField.name, someField.defaultValue);
      }
    }

    onChange(newModel);
  };

  private renderNumberInput(field: Field<T>): JSX.Element {
    const { model, large } = this.props;
    return (
      <NumericInput
        value={deepGet(model as any, field.name) || field.defaultValue}
        onValueChange={(valueAsNumber: number, valueAsString: string) => {
          if (valueAsString === '') {
            this.fieldChange(field, undefined);
            return;
          }
          if (isNaN(valueAsNumber)) return;
          this.fieldChange(field, valueAsNumber);
        }}
        min={field.min || 0}
        fill
        large={large}
        disabled={field.disabled}
        placeholder={field.placeholder}
      />
    );
  }

  private renderSizeBytesInput(field: Field<T>): JSX.Element {
    const { model, large } = this.props;
    return (
      <NumericInput
        value={deepGet(model as any, field.name) || field.defaultValue}
        onValueChange={(v: number) => {
          if (isNaN(v)) return;
          this.fieldChange(field, v);
        }}
        min={0}
        stepSize={1000}
        majorStepSize={1000000}
        large={large}
        disabled={field.disabled}
      />
    );
  }

  private renderStringInput(field: Field<T>, sanitize?: (str: string) => string): JSX.Element {
    const { model, large } = this.props;

    const modalValue = deepGet(model as any, field.name);
    return (
      <SuggestibleInput
        value={modalValue != null ? modalValue : field.defaultValue || ''}
        onValueChange={v => {
          if (sanitize) v = sanitize(v);
          this.fieldChange(field, v);
        }}
        onBlur={() => {
          if (modalValue === '') this.fieldChange(field, undefined);
        }}
        placeholder={field.placeholder}
        suggestions={field.suggestions}
        large={large}
        disabled={field.disabled}
      />
    );
  }

  private renderBooleanInput(field: Field<T>): JSX.Element {
    const { model, large } = this.props;
    let curValue = deepGet(model as any, field.name);
    if (curValue == null) curValue = field.defaultValue;
    return (
      <HTMLSelect
        value={curValue === true ? 'True' : 'False'}
        onChange={(e: any) => {
          const v = e.currentTarget.value === 'True';
          this.fieldChange(field, v);
        }}
        large={large}
        disabled={field.disabled}
      >
        <option value="True">True</option>
        <option value="False">False</option>
      </HTMLSelect>
    );
  }

  private renderJSONInput(field: Field<T>): JSX.Element {
    const { model, updateJSONValidity } = this.props;
    const { jsonInputsValidity } = this.state;

    const updateInputValidity = (e: any) => {
      if (updateJSONValidity) {
        const newJSONInputValidity = Object.assign({}, jsonInputsValidity, { [field.name]: e });
        this.setState({
          jsonInputsValidity: newJSONInputValidity,
        });
        const allJSONValid: boolean = Object.keys(newJSONInputValidity).every(
          property => newJSONInputValidity[property] === true,
        );
        updateJSONValidity(allJSONValid);
      }
    };

    return (
      <JSONInput
        value={deepGet(model as any, field.name)}
        onChange={(v: any) => this.fieldChange(field, v)}
        updateInputValidity={updateInputValidity}
        placeholder={field.placeholder}
      />
    );
  }

  private renderStringArrayInput(field: Field<T>): JSX.Element {
    const { model, large } = this.props;
    return (
      <ArrayInput
        values={deepGet(model as any, field.name) || []}
        onChange={(v: any) => {
          this.fieldChange(field, v);
        }}
        placeholder={field.placeholder}
        large={large}
        disabled={field.disabled}
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
        return this.renderJSONInput(field);
      default:
        throw new Error(`unknown field type '${field.type}'`);
    }
  }

  private renderField = (field: Field<T>) => {
    const { model } = this.props;
    if (!model) return null;
    if (field.isDefined && !field.isDefined(model)) return null;

    const label = field.label || AutoForm.makeLabelName(field.name);
    return (
      <FormGroup
        key={field.name}
        label={label}
        labelInfo={
          field.info && (
            <Popover
              content={<div className="label-info-text">{field.info}</div>}
              position="left-bottom"
            >
              <Icon icon={IconNames.INFO_SIGN} iconSize={14} />
            </Popover>
          )
        }
      >
        {this.renderFieldInput(field)}
      </FormGroup>
    );
  };

  renderCustom() {
    const { model } = this.props;

    return (
      <FormGroup label="Custom" key="custom">
        <JSONInput value={model} onChange={this.modelChange} />
      </FormGroup>
    );
  }

  render(): JSX.Element {
    const { fields, model, showCustom } = this.props;
    return (
      <div className="auto-form">
        {model && fields.map(this.renderField)}
        {model && showCustom && showCustom(model) && this.renderCustom()}
      </div>
    );
  }
}
