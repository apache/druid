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

import { Button } from '@blueprintjs/core';
import classNames from 'classnames';
import * as React from 'react';
import AceEditor from "react-ace";

import { parseStringToJSON, stringifyJSON, validJson } from "../utils";

import './filler.scss';

export const IconNames = {
  ERROR: "error" as "error",
  PLUS: "plus" as "plus",
  REFRESH: "refresh" as "refresh",
  APPLICATION: "application" as "application",
  GRAPH: "graph" as "graph",
  MAP: "map" as "map",
  TH: "th" as "th",
  USER: "user" as "user",
  GIT_BRANCH: "git-branch" as "git-branch",
  COG: "cog" as "cog",
  MULTI_SELECT: "multi-select" as "multi-select",
  STACKED_CHART: "stacked-chart" as "stacked-chart",
  GANTT_CHART: "gantt-chart" as "gantt-chart",
  DATABASE: "database" as "database",
  SETTINGS: "settings" as "settings",
  HELP: "help" as "help",
  SHARE: "share" as "share",
  CROSS: "cross" as "cross",
  ARROW_LEFT: "arrow-left" as "arrow-left",
  CARET_RIGHT: "caret-right" as "caret-right",
  TICK: "tick" as "tick",
  ARROW_RIGHT: "right-arrow" as "right-arrow",
  TRASH: "trash" as "trash",
  CARET_DOWN: "caret-down" as "caret-down",
  ARROW_UP: "arrow-up" as "arrow-up",
  ARROW_DOWN: "arrow-down" as "arrow-down",
  PROPERTIES: "properties" as "properties",
  BUILD: "build" as "build",
  WRENCH: "wrench" as "wrench"
};
export type IconNames = typeof IconNames[keyof typeof IconNames];

export class H5 extends React.Component<{}, {}> {
  render() {
    const { children } = this.props;
    return <h5>{children}</h5>;
  }
}

export class Card extends React.Component<{ interactive?: boolean }, {}> {
  render() {
    const { interactive, children } = this.props;
    return <div className={classNames("pt-card", { 'pt-interactive': interactive })}>
      {children}
    </div>;
  }
}

export class Icon extends React.Component<{ icon: string, color?: string }, {}> {
  render() {
    const { color, icon } = this.props;
    return <span className={classNames('pt-icon-standard', 'pt-icon-' + icon)} style={{ color }}/>;
  }
}

export class ControlGroup extends React.Component<{}, {}> {
  render() {
    return <div className="pt-control-group" {...this.props}/>;
  }
}

export class ButtonGroup extends React.Component<{ vertical?: boolean, fixed?: boolean }, {}> {
  render() {
    const { vertical, fixed, children } = this.props;
    return <div className={classNames("pt-button-group", { 'pt-vertical': vertical, 'pt-fixed': fixed })}>
      {children}
    </div>;
  }
}

export class Label extends React.Component<{}, {}> {
  render() {
    const { children } = this.props;
    return <label className="pt-label">{children}</label>;
  }
}

export class FormGroup extends React.Component<{ className?: string, label?: string }, {}> {
  render() {
    const { className, label, children } = this.props;
    return <div className={classNames("form-group", className)}>
      {label ? <Label>{label}</Label> : null}
      {children}
    </div>;
  }
}

export const Alignment = {
  LEFT: "left" as "left",
  RIGHT: "right" as "right"
};
export type Alignment = typeof Alignment[keyof typeof Alignment];

export class Navbar extends React.Component<{ className?: string }, {}> {
  render() {
    const { className, children } = this.props;
    return <nav className={classNames("pt-navbar", className)}>
      {children}
    </nav>;
  }
}

export class NavbarGroup extends React.Component<{ align: Alignment }, {}> {
  render() {
    const { align, children } = this.props;
    return <div className={classNames('pt-navbar-group', 'pt-align-' + align)}>
      {children}
    </div>;
  }
}

export class NavbarDivider extends React.Component<{}, {}> {
  render() {
    return <span className="pt-navbar-divider"/>;
  }
}

export class HTMLSelect extends React.Component<{ key?: string; style?: any; onChange: any; value: any; fill?: boolean }, {}> {
  render() {
    const { key, style, onChange, value, fill, children } = this.props;
    return <div className={classNames("pt-select", { 'pt-fill': fill })} key={key} style={style}>
      <select onChange={onChange} value={value}>{children}</select>
    </div>;
  }
}

export class TextArea extends React.Component<{ className?: string; onChange?: any; value?: string }, {}> {
  render() {
    const { className, value, onChange } = this.props;
    return <textarea
      className={classNames("pt-input", className)}
      value={value}
      onChange={onChange}
    />;
  }
}

export interface NumericInputProps {
  value: number | null;
  onValueChange: any;
  min?: number;
  max?: number;
  stepSize?: number;
  majorStepSize?: number;
}

export class NumericInput extends React.Component<NumericInputProps, { stringValue: string }> {

  static defaultProps = {
    stepSize: 1,
    majorStepSize: 10
  };

  constructor(props: NumericInputProps) {
    super(props);
    this.state = {
      stringValue: typeof props.value === 'number' ? String(props.value) : ''
    };
  }

  private constrain(n: number): number {
    const { min, max } = this.props;
    if (typeof min === 'number') n = Math.max(n, min);
    if (typeof max === 'number') n = Math.min(n, max);
    return n;
  }

  private handleChange = (e: any) => {
    let stringValue = e.target.value.replace(/[^\d.+-]/g, '');
    let numValue = parseFloat(stringValue);
    if (isNaN(numValue)) {
      this.setState({ stringValue });
    } else {
      numValue = this.constrain(numValue);
      stringValue = String(numValue);
      this.setState({ stringValue });
      this.props.onValueChange(numValue);
    }
  }

  private handleClick = (e: any, direction: number) => {
    const { stepSize, majorStepSize } = this.props;
    const { stringValue } = this.state;
    const diff = direction * (e.shiftKey ? majorStepSize as number : stepSize as number);
    const numValue = this.constrain((parseFloat(stringValue) || 0) + diff);
    this.setState({ stringValue: String(numValue) });
    this.props.onValueChange(numValue);
  }

  render() {
    const { stringValue } = this.state;

    return <ControlGroup>
      <input className="pt-input" value={stringValue} onChange={this.handleChange}/>
      <ButtonGroup fixed>
        <Button iconName="caret-up" onClick={(e: any) => this.handleClick(e, +1)}/>
        <Button iconName="caret-down" onClick={(e: any) => this.handleClick(e, -1)}/>
      </ButtonGroup>
    </ControlGroup>;
  }
}

export interface TagInputProps {
  values: string[];
  onChange: any;
  fill?: boolean;
}

export class TagInput extends React.Component<TagInputProps, { stringValue: string }> {
  constructor(props: TagInputProps) {
    super(props);
    this.state = {
      stringValue: Array.isArray(props.values) ? props.values.join(', ') : ''
    };
  }

  handleChange = (e: any) => {
    const stringValue = e.target.value;
    const newValues = stringValue.split(',').map((v: string) => v.trim());
    const newValuesFiltered = newValues.filter(Boolean);
    this.setState({
      stringValue: newValues.length === newValuesFiltered.length ? newValues.join(', ') : stringValue
    });
    this.props.onChange(newValuesFiltered);
  }

  render() {
    const { fill } = this.props;
    const { stringValue } = this.state;
    return <input
      className={classNames("pt-input", {'pt-fill': fill })}
      value={stringValue}
      onChange={this.handleChange}
    />;
  }
}

interface JSONInputProps extends React.Props<any> {
  onChange: (newJSONValue: any) => void;
  value: any;
  updateInputValidity: (valueValid: boolean) => void;
}

interface JSONInputState {
  stringValue: string;
}

export class JSONInput extends React.Component<JSONInputProps, JSONInputState> {
  constructor(props: JSONInputProps) {
    super(props);
    this.state = {
      stringValue: ""
    };
  }

  componentDidMount(): void {
    const { value } = this.props;
    const stringValue = stringifyJSON(value);
    this.setState({
      stringValue
    });
  }

  componentWillReceiveProps(nextProps: JSONInputProps): void {
    if (JSON.stringify(nextProps.value) !== JSON.stringify(this.props.value)) {
      this.setState({
        stringValue: stringifyJSON(nextProps.value)
      });
    }
  }

  render() {
    const { onChange, updateInputValidity } = this.props;
    const { stringValue } = this.state;
    return <AceEditor
      className={"bp3-fill"}
      key={"hjson"}
      mode={"hjson"}
      theme="solarized_dark"
      name="ace-editor"
      onChange={(e: string) => {
        this.setState({stringValue: e});
        if (validJson(e) || e === "") onChange(parseStringToJSON(e));
        updateInputValidity(validJson(e) || e === '');
      }}
      focus
      fontSize={12}
      width={'100%'}
      height={"8vh"}
      showPrintMargin={false}
      showGutter={false}
      value={stringValue}
      editorProps={{
        $blockScrolling: Infinity
      }}
      setOptions={{
        enableBasicAutocompletion: false,
        enableLiveAutocompletion: false,
        showLineNumbers: false,
        tabSize: 2
      }}
    />;
  }
}
