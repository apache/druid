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

const React = require('react');

class Footer extends React.Component {
  render() {
    return (
      <footer className="nav-footer druid-footer" id="footer">
        <div className="container">
          <div className="text-center">
            <p>
              <a href="/technology">Technology</a>&ensp;·&ensp;
              <a href="/use-cases">Use Cases</a>&ensp;·&ensp;
              <a href="/druid-powered">Powered by Druid</a>&ensp;·&ensp;
              <a href="/docs/latest">Docs</a>&ensp;·&ensp;
              <a href="/community/">Community</a>&ensp;·&ensp;
              <a href="/downloads.html">Download</a>&ensp;·&ensp;
              <a href="/faq">FAQ</a>
            </p>
          </div>
          <div className="text-center">
            <a
              title="Join the user group"
              href="https://groups.google.com/forum/#!forum/druid-user"
              target="_blank"
            >
              <span className="fa fa-comments" />
            </a>
            &ensp;·&ensp;
            <a title="Follow Druid" href="https://twitter.com/druidio" target="_blank">
              <span className="fab fa-twitter" />
            </a>
            &ensp;·&ensp;
            <a
              title="Download via Apache"
              href="https://www.apache.org/dyn/closer.cgi?path=/incubator/druid/{{ site.druid_versions[0].versions[0].version }}/apache-druid-{{ site.druid_versions[0].versions[0].version }}-bin.tar.gz"
              target="_blank"
            >
              <span className="fas fa-feather" />
            </a>
            &ensp;·&ensp;
            <a title="GitHub" href="https://github.com/apache/incubator-druid" target="_blank">
              <span className="fab fa-github" />
            </a>
          </div>
          <div className="text-center license">
            Copyright © 2019{' '}
            <a href="https://www.apache.org/" target="_blank">
              Apache Software Foundation
            </a>
            .<br />
            Except where otherwise noted, licensed under{' '}
            <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">
              CC BY-SA 4.0
            </a>
            .<br />
            Apache Druid, Druid, and the Druid logo are either registered trademarks or trademarks
            of The Apache Software Foundation in the United States and other countries.
          </div>
        </div>
      </footer>
    );
  }
}

module.exports = Footer;
