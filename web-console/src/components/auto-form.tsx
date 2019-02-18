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

import { resolveSrv } from 'dns';
import * as React from 'react';
import axios from 'axios';
import {
  FormGroup,
  Button,
  InputGroup,
  Dialog,
  NumericInput,
  Classes,
  Tooltip,
  AnchorButton,
  TagInput,
  Intent,
  ButtonGroup,
  HTMLSelect
} from "@blueprintjs/core";

interface Field {
  name: string;
  label?: string;
  type: 'number' | 'size-bytes' | 'string' | 'boolean' | 'string-array';
  min?: number;
}

export interface AutoFormProps<T> extends React.Props<any> {
  fields: Field[];
  model: T | null,
  onChange: (newValue: T) => void
}

export interface AutoFormState<T> {
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
    }
  }

  private renderNumberInput(field: Field): JSX.Element {
    const { model, onChange } = this.props;
    return <NumericInput
      value={(model as any)[field.name]}
      onValueChange={v => {
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
      onValueChange={v => {
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
      options={["True", "False"]}
      value={(model as any)[field.name] === true ? "True" : "False"}
      onChange={e => {
        onChange(Object.assign({}, model, { [field.name]: e.currentTarget.value === "True" }));
      }}
    />
  }

  private renderStringArrayInput(field: Field): JSX.Element {
    const { model, onChange } = this.props;
    const label = field.label || AutoForm.makeLabelName(field.name);
    return <TagInput
      values={(model as any)[field.name] || []}
      onChange={(v: any) => {
        onChange(Object.assign({}, model, { [field.name]: v }));
      }}
      addOnBlur={true}
    />;
  }

  renderFieldInput(field: Field) {
    switch (field.type) {
      case 'number': return this.renderNumberInput(field);
      case 'size-bytes': return this.renderSizeBytesInput(field);
      case 'string': return this.renderStringInput(field);
      case 'boolean': return this.renderBooleanInput(field);
      case 'string-array': return this.renderStringArrayInput(field);
      default: throw new Error(`unknown field type '${field.type}'`);
    }
  }

  renderField(field: Field) {
    const label = field.label || AutoForm.makeLabelName(field.name);
    return <FormGroup label={label} key={field.name}>
      {this.renderFieldInput(field)}
    </FormGroup>
  }

  render() {
    const { fields, model } = this.props;

    return <div className="auto-form">
      {model && fields.map(field => this.renderField(field))}
    </div>
  }
}
