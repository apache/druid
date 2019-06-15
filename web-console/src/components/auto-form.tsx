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

import { InputGroup } from "@blueprintjs/core";
import { FormGroup, HTMLSelect, NumericInput, TagInput } from "@blueprintjs/core";
import * as React from 'react';

import { JSONInput } from './json-input';

import "./auto-form.scss";

interface Field {
  name: string;
  label?: string;
  type: 'number' | 'size-bytes' | 'string' | 'boolean' | 'string-array' | 'json';
  min?: number;
}

export interface AutoFormProps<T> extends React.Props<any> {
  fields: Field[];
  model: T | null;
  onChange: (newValue: T) => void;
  updateJSONValidity?: (jsonValidity: boolean) => void;
}

export interface AutoFormState<T> {
  jsonInputsValidity: any;
}

export class AutoForm<T> extends React.Component<AutoFormProps<T>, AutoFormState<T>> {
  static makeLabelName(label: string): string {
    let newLabel = label.split(/(?=[A-Z])/).map(s => s.toLowerCase()).join(" ");
    newLabel = newLabel[0].toUpperCase() + newLabel.slice(1);
    return newLabel;
  }

  constructor(props: AutoFormProps<T>) {
    super(props);
    this.state = {
      jsonInputsValidity: {}
    };
  }

  private renderNumberInput(field: Field): JSX.Element {
    const { model, onChange } = this.props;
    return <NumericInput
      value={(model as any)[field.name]}
      onValueChange={(v: any) => {
        if (isNaN(v)) return;
        onChange(Object.assign({}, model, { [field.name]: v }));
      }}
      min={field.min || 0}
    />;
  }

  private renderSizeBytesInput(field: Field): JSX.Element {
    const { model, onChange } = this.props;
    return <NumericInput
      value={(model as any)[field.name]}
      onValueChange={(v: number) => {
        if (isNaN(v)) return;
        onChange(Object.assign({}, model, { [field.name]: v }));
      }}
      min={0}
      stepSize={1000}
      majorStepSize={1000000}
    />;
  }

  private renderStringInput(field: Field): JSX.Element {
    const { model, onChange } = this.props;
    return <InputGroup
      value={(model as any)[field.name]}
      onChange={(v: any) => {
        onChange(Object.assign({}, model, { [field.name]: v }));
      }}
    />;
  }

  private renderBooleanInput(field: Field): JSX.Element {
    const { model, onChange } = this.props;
    return <HTMLSelect
      value={(model as any)[field.name] === true ? "True" : "False"}
      onChange={(e: any) => {
        onChange(Object.assign({}, model, { [field.name]: e.currentTarget.value === "True" }));
      }}
    >
      <option value="True">True</option>
      <option value="False">False</option>
    </HTMLSelect>;
  }

  private renderJSONInput(field: Field): JSX.Element {
    const { model, onChange,  updateJSONValidity } = this.props;
    const { jsonInputsValidity } = this.state;

    const updateInputValidity = (e: any) => {
      if (updateJSONValidity) {
        const newJSONInputValidity = Object.assign({}, jsonInputsValidity, { [field.name]: e});
        this.setState({
          jsonInputsValidity: newJSONInputValidity
        });
        const allJSONValid: boolean = Object.keys(newJSONInputValidity).every(property => newJSONInputValidity[property] === true);
        updateJSONValidity(allJSONValid);
      }
    };

    return <JSONInput
      value={(model as any)[field.name]}
      onChange={(e: any) => onChange(Object.assign({}, model, { [field.name]: e}))}
      updateInputValidity={updateInputValidity}
    />;
  }

  private renderStringArrayInput(field: Field): JSX.Element {
    const { model, onChange } = this.props;
    return <TagInput
      values={(model as any)[field.name] || []}
      onChange={(v: any) => {
        onChange(Object.assign({}, model, { [field.name]: v }));
      }}
      fill
    />;
  }

  renderFieldInput(field: Field) {
    switch (field.type) {
      case 'number': return this.renderNumberInput(field);
      case 'size-bytes': return this.renderSizeBytesInput(field);
      case 'string': return this.renderStringInput(field);
      case 'boolean': return this.renderBooleanInput(field);
      case 'string-array': return this.renderStringArrayInput(field);
      case 'json': return this.renderJSONInput(field);
      default: throw new Error(`unknown field type '${field.type}'`);
    }
  }

  renderField(field: Field) {
    const label = field.label || AutoForm.makeLabelName(field.name);
    return <FormGroup label={label} key={field.name}>
      {this.renderFieldInput(field)}
    </FormGroup>;
  }

  render() {
    const { fields, model } = this.props;
    return <div className="auto-form">
      {model && fields.map(field => this.renderField(field))}
    </div>;
  }
}
