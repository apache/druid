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

import { render } from '@testing-library/react';

import { SplitterLayout } from './splitter-layout';

describe('SplitterLayout', () => {
  it('matches snapshot one child', () => {
    const splitterLayout = (
      <SplitterLayout primaryMinSize={100} secondaryInitialSize={50} secondaryMinSize={10}>
        <div className="child1" />
      </SplitterLayout>
    );

    const { container } = render(splitterLayout);
    expect(container.firstChild).toMatchSnapshot();
  });

  it('matches snapshot two children', () => {
    const splitterLayout = (
      <SplitterLayout
        vertical
        percentage
        primaryMinSize={100}
        secondaryInitialSize={50}
        secondaryMinSize={10}
      >
        <div className="child1" />
        <div className="child2" />
      </SplitterLayout>
    );

    const { container } = render(splitterLayout);
    expect(container.firstChild).toMatchSnapshot();
  });
});
