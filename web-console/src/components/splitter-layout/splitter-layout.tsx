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

// Originally copied from https://github.com/zesik/react-splitter-layout/blob/master/src/components/SplitterLayout.jsx and heavily refactored

import classNames from 'classnames';
import type { ReactNode } from 'react';
import { Children, Component } from 'react';

import { clamp } from '../../utils';

import { LayoutPane } from './layout-pane';

import './splitter-layout.scss';

function clearSelection() {
  if (window.getSelection) {
    const selection = window.getSelection();
    if (selection) {
      if (selection.empty) {
        selection.empty();
      } else if (selection.removeAllRanges) {
        selection.removeAllRanges();
      }
    }
  }
}

export interface SplitterLayoutProps {
  className?: string;
  vertical?: boolean;
  percentage?: boolean;
  primaryIndex?: 0 | 1;
  primaryMinSize?: number;
  secondaryInitialSize: number;
  secondaryMinSize?: number;
  secondaryMaxSize?: number;
  splitterSize?: number;
  onSecondaryPaneSizeChange?: (size: number) => void;
  children: ReactNode | ReactNode[];
}

interface SplitterLayoutState {
  secondaryPaneSize: number;
  resizing: boolean;
}

export class SplitterLayout extends Component<SplitterLayoutProps, SplitterLayoutState> {
  private container: HTMLDivElement | null = null;
  private splitter: HTMLDivElement | null = null;

  constructor(props: SplitterLayoutProps) {
    super(props);

    this.handleResize = this.handleResize.bind(this);
    this.handleMouseMove = this.handleMouseMove.bind(this);
    this.handleMouseUp = this.handleMouseUp.bind(this);
    this.handleTouchMove = this.handleTouchMove.bind(this);
    this.handleSplitterMouseDown = this.handleSplitterMouseDown.bind(this);

    this.state = {
      secondaryPaneSize: props.secondaryInitialSize,
      resizing: false,
    };
  }

  componentDidMount() {
    window.addEventListener('resize', this.handleResize);
    document.addEventListener('mouseup', this.handleMouseUp);
    document.addEventListener('mousemove', this.handleMouseMove);
    document.addEventListener('touchend', this.handleMouseUp);
    document.addEventListener('touchmove', this.handleTouchMove);
  }

  componentDidUpdate(_prevProps: SplitterLayoutProps, prevState: SplitterLayoutState) {
    if (prevState.secondaryPaneSize !== this.state.secondaryPaneSize) {
      this.props.onSecondaryPaneSizeChange?.(this.state.secondaryPaneSize);
    }
  }

  componentWillUnmount() {
    window.removeEventListener('resize', this.handleResize);
    document.removeEventListener('mouseup', this.handleMouseUp);
    document.removeEventListener('mousemove', this.handleMouseMove);
    document.removeEventListener('touchend', this.handleMouseUp);
    document.removeEventListener('touchmove', this.handleTouchMove);
  }

  getSecondaryPaneSize(
    containerRect: DOMRect,
    splitterRect: DOMRect,
    clientPosition: { top: number; left: number },
    offsetMouse: boolean,
  ) {
    const {
      vertical,
      percentage,
      primaryIndex,
      primaryMinSize = 0,
      secondaryMinSize = 0,
      secondaryMaxSize = Infinity,
    } = this.props;

    let totalSize;
    let splitterSize;
    let offset;
    if (vertical) {
      totalSize = containerRect.height;
      splitterSize = splitterRect.height;
      offset = clientPosition.top - containerRect.top;
    } else {
      totalSize = containerRect.width;
      splitterSize = splitterRect.width;
      offset = clientPosition.left - containerRect.left;
    }
    if (offsetMouse) {
      offset -= splitterSize / 2;
    }
    offset = clamp(offset, 0, totalSize - splitterSize);

    let secondaryPaneSize = primaryIndex === 1 ? offset : totalSize - splitterSize - offset;
    if (percentage) {
      secondaryPaneSize = (secondaryPaneSize * 100) / totalSize;
      splitterSize = (splitterSize * 100) / totalSize;
      totalSize = 100;
    }

    return clamp(
      secondaryPaneSize,
      secondaryMinSize,
      Math.min(secondaryMaxSize, totalSize - splitterSize - primaryMinSize),
    );
  }

  handleResize() {
    if (this.container && this.splitter && !this.props.percentage) {
      const containerRect = this.container.getBoundingClientRect();
      const splitterRect = this.splitter.getBoundingClientRect();
      const secondaryPaneSize = this.getSecondaryPaneSize(
        containerRect,
        splitterRect,
        {
          left: splitterRect.left,
          top: splitterRect.top,
        },
        false,
      );
      this.setState({ secondaryPaneSize });
    }
  }

  handleMouseMove(e: MouseEvent | Touch) {
    if (this.container && this.splitter && this.state.resizing) {
      const containerRect = this.container.getBoundingClientRect();
      const splitterRect = this.splitter.getBoundingClientRect();
      const secondaryPaneSize = this.getSecondaryPaneSize(
        containerRect,
        splitterRect,
        {
          left: e.clientX,
          top: e.clientY,
        },
        true,
      );
      clearSelection();
      this.setState({ secondaryPaneSize });
    }
  }

  handleTouchMove(e: TouchEvent) {
    this.handleMouseMove(e.changedTouches[0]);
  }

  handleSplitterMouseDown() {
    clearSelection();
    this.setState({ resizing: true });
  }

  handleMouseUp() {
    this.setState(prevState => (prevState.resizing ? { resizing: false } : null));
  }

  render() {
    const { className, vertical, percentage, primaryIndex, splitterSize, children } = this.props;
    const { resizing } = this.state;

    const childrenArray = Children.toArray(children).slice(0, 2);
    if (childrenArray.length === 0) return null;

    const effectivePrimaryIndex = primaryIndex === 1 ? 1 : 0;
    const wrappedChildren = childrenArray.map((child, i) => {
      const isSecondary = childrenArray.length > 1 && i !== effectivePrimaryIndex;
      return (
        <LayoutPane
          key={isSecondary ? 'secondary' : 'primary'}
          vertical={vertical}
          percentage={percentage}
          size={isSecondary ? this.state.secondaryPaneSize : undefined}
        >
          {child}
        </LayoutPane>
      );
    });

    return (
      <div
        className={classNames(
          'splitter-layout',
          className,
          vertical ? 'splitter-layout-vertical' : 'splitter-layout-horizontal',
          { 'layout-changing': resizing },
        )}
        ref={c => {
          this.container = c;
        }}
      >
        {wrappedChildren[0]}
        {wrappedChildren.length > 1 && (
          <div
            role="separator"
            className="layout-splitter"
            ref={c => {
              this.splitter = c;
            }}
            onMouseDown={this.handleSplitterMouseDown}
            onTouchStart={this.handleSplitterMouseDown}
            style={splitterSize ? { [vertical ? 'height' : 'width']: splitterSize } : undefined}
          />
        )}
        {wrappedChildren[1]}
      </div>
    );
  }
}
