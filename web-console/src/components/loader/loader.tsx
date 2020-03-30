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

import React from 'react';

import './loader.scss';

export interface LoaderProps {
  loadingText?: string;
  loading?: boolean; // This is needed so that this component can be used as a LoadingComponent in react table
}

export const Loader = React.memo(function Loader(props: LoaderProps) {
  const { loadingText, loading } = props;
  if (!loading) return null;

  return (
    <div className="loader">
      <div className="loader-logo">
        <svg viewBox="0 0 100 100">
          <path
            className="one"
            d="M54.2,69.8h-2.7c-0.7,0-1.3-0.6-1.3-1.3c0-0.7,0.6-1.3,1.3-1.3h2.7c11.5,0,23.8-7.4,23.8-23.7
          c0-9.1-6.9-15.8-16.4-15.8H38c-0.7,0-1.3-0.6-1.3-1.3s0.6-1.3,1.3-1.3h23.6c5.3,0,10.1,1.9,13.6,5.3c3.5,3.4,5.4,8,5.4,13.1
          c0,6.6-2.3,13-6.3,17.7C69.5,66.8,62.5,69.8,54.2,69.8z"
          />
          <path
            className="two"
            d="M55.7,59.5h-26c-0.7,0-1.3-0.6-1.3-1.3c0-0.7,0.6-1.3,1.3-1.3h26c7.5,0,11.5-5.8,11.5-11.5
            c0-4.2-3.2-7.3-7.7-7.3h-26c-0.7,0-1.3-0.6-1.3-1.3s0.6-1.3,1.3-1.3h26c5.9,0,10.3,4.3,10.3,9.9c0,3.7-1.3,7.2-3.7,9.8
            C63.5,58,59.9,59.5,55.7,59.5z"
          />
          <path
            className="three"
            d="M27.2,38h-6.3c-0.7,0-1.3-0.6-1.3-1.3s0.6-1.3,1.3-1.3h6.3c0.7,0,1.3,0.6,1.3,1.3S27.9,38,27.2,38z"
          />
          <path
            className="four"
            d="M45.1,69.8h-5.8c-0.7,0-1.3-0.6-1.3-1.3c0-0.7,0.6-1.3,1.3-1.3h5.8c0.7,0,1.3,0.6,1.3,1.3
            C46.4,69.2,45.8,69.8,45.1,69.8z"
          />
        </svg>
        {loadingText ? <div className="label">{loadingText}</div> : null}
      </div>
    </div>
  );
});
