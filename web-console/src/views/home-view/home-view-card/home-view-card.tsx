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

import { Card, H5, Icon, IconName } from '@blueprintjs/core';
import classNames from 'classnames';
import React, { ReactNode } from 'react';

import './home-view-card.scss';

export interface HomeViewCardProps {
  className: string;
  onClick?: () => void;
  href?: string;
  icon: IconName;
  title: string;
  loading: boolean;
  error: string | undefined;
  children?: ReactNode;
}

export function HomeViewCard(props: HomeViewCardProps) {
  const { className, onClick, href, icon, title, loading, error, children } = props;

  return (
    <a
      className={classNames('home-view-card', className)}
      onClick={onClick}
      href={href}
      target={href && href[0] === '/' ? '_blank' : undefined}
    >
      <Card interactive>
        <H5>
          <Icon color="#bfccd5" icon={icon} />
          &nbsp;{title}
        </H5>
        {loading ? <p>Loading...</p> : error ? `Error: ${error}` : children}
      </Card>
    </a>
  );
}
