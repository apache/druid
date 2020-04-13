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

import { Button, ButtonGroup, IButtonProps, Popover, Radio, RadioGroup } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';

import { localStorageGet, LocalStorageKeys, localStorageSet } from '../../utils';

import './timed-button.scss';

export interface Interval {
  label: string;
  value: number;
}

export interface TimedButtonProps extends IButtonProps {
  intervals: Interval[];
  onRefresh: (auto: boolean) => void;
  localStorageKey?: LocalStorageKeys;
  label: string;
  defaultValue: number;
}

export interface TimedButtonState {
  interval: number;
}

export class TimedButton extends React.PureComponent<TimedButtonProps, TimedButtonState> {
  constructor(props: TimedButtonProps, context: any) {
    super(props, context);
    this.state = {
      interval:
        this.props.localStorageKey && localStorageGet(this.props.localStorageKey)
          ? Number(localStorageGet(this.props.localStorageKey))
          : this.props.defaultValue,
    };
  }

  private timer: any;

  componentDidMount(): void {
    if (this.state.interval) {
      this.timer = setTimeout(() => {
        this.continuousRefresh(this.state.interval);
      }, this.state.interval);
    }
  }

  componentWillUnmount(): void {
    this.clearTimer();
  }

  clearTimer() {
    if (this.timer) {
      clearTimeout(this.timer);
    }
    this.timer = undefined;
  }

  continuousRefresh = (selectedInterval: number) => {
    if (selectedInterval) {
      this.timer = setTimeout(() => {
        this.props.onRefresh(true);
        this.continuousRefresh(selectedInterval);
      }, selectedInterval);
    }
  };

  handleSelection = (e: any) => {
    const selectedInterval = Number(e.currentTarget.value);
    this.clearTimer();
    this.setState({ interval: selectedInterval });
    if (this.props.localStorageKey) {
      localStorageSet(this.props.localStorageKey, String(selectedInterval));
    }
    this.continuousRefresh(selectedInterval);
  };

  render(): JSX.Element {
    const {
      label,
      intervals,
      onRefresh,
      type,
      text,
      icon,
      defaultValue,
      localStorageKey,
      ...other
    } = this.props;
    const { interval } = this.state;

    return (
      <ButtonGroup>
        <Button {...other} text={text} icon={icon} onClick={() => onRefresh(false)} />
        <Popover
          content={
            <RadioGroup
              label={label}
              className="timed-button"
              onChange={this.handleSelection}
              selectedValue={interval}
            >
              {intervals.map((interval: any) => (
                <Radio label={interval.label} value={interval.value} key={interval.label} />
              ))}
            </RadioGroup>
          }
        >
          <Button {...other} rightIcon={IconNames.CARET_DOWN} />
        </Popover>
      </ButtonGroup>
    );
  }
}
