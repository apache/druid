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

import type React from 'react';
import ShallowRenderer from 'react-shallow-renderer';

/**
 * Shallowly renders a React element for the purpose snapshot testing.
 *
 * @param node - The React element to render.
 * @returns - An opaque object that can be serialized to a snapshot.
 */
export function shallow(node: React.ReactElement): object {
  const renderer = new ShallowRenderer();
  renderer.render(node);

  const instance = renderer.getMountedInstance();
  if (instance?.componentDidMount) {
    // Call componentDidMount if it exists -- this isn't done by default
    // because DOM refs are not available in shallow rendering, but we
    // don't rely on those in our tests.
    instance.componentDidMount();
  }

  return renderer.getRenderOutput();
}
