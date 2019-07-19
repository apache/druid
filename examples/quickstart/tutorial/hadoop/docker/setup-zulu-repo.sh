#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# Script to set up the Azul Zulu JDK yum repository.
#

# Hardcode GPG key so we don't have to fetch it over http.
cat <<'EOT' > /root/RPM-GPG-KEY-azulsystems
-----BEGIN PGP PUBLIC KEY BLOCK-----
Version: GnuPG v1

mQINBFNgFa8BEADTL/REB10M+TfiZOtFHqL5LHKkzTMn/O2r5iIqXGhi6iwZazFs
9S5g1eU7WMen5Xp9AREs+OvaHx91onPZ7ZiP7VpZ6ZdwWrnVk1Y/HfI59tWxmNYW
DmKYBGMj4EUpFPSE9EnFj7dm1WdlCvpognCwZQl9D3BseGqN7OLHfwqqmOlbYN9h
HYkT+CaqOoWDIGMB3UkBlMr0GuujEP8N1gxg7EOcSCsZH5aKtXubdUlVSphfAAwD
z4MviB39J22sPBnKmaOT3TUTO5vGeKtC9BAvtgA82jY2TtCEjetnfK/qtzj/6j2N
xVUbHQydwNQVRU92A7334YvCbn3xUUNI0WOscdmfpgCU0Z9Gb2IqDb9cMjgUi8F6
MG/QY9/CZjX62XrHRPm3aXsCJOVh/PO1sl2A/rvv8AkpJKYyhm6T8OBFptCsA3V4
Oic7ZyYhqV0u2r4NON+1MoUeuuoeY2tIrbRxe3ffVOxPzrESzSbc8LC2tYaP+wGd
W0f57/CoDkUzlvpReCUI1Bv5zP4/jhC63Rh6lffvSf2tQLwOsf5ivPhUtwUfOQjg
v9P8Wc8K7XZpSOMnDZuDe9wuvB/DiH/P5yiTs2RGsbDdRh5iPfwbtf2+IX6h2lNZ
XiDKt9Gc26uzeJRx/c7+sLunxq6DLIYvrsEipVI9frHIHV6fFTmqMJY6SwARAQAB
tEdBenVsIFN5c3RlbXMsIEluYy4gKFBhY2thZ2Ugc2lnbmluZyBrZXkuKSA8cGtp
LXNpZ25pbmdAYXp1bHN5c3RlbXMuY29tPokCOAQTAQIAIgUCU2AVrwIbAwYLCQgH
AwIGFQgCCQoLBBYCAwECHgECF4AACgkQsZmDYSGb2cnJ8xAAz1V1PJnfOyaRIP2N
Ho2uRwGdPsA4eFMXb4Z08eGjDMD3b9WW3D0XnCLbJpaZ6klz0W0s2tcYSneTBaSs
RAqxgJgBZ5ZMXtrrHld/5qFoBbStLZLefmcPhnfvamwHDCTLUex8NIAI1u3e9Rhb
5fbH+gpuYpwHX7hz0FOfpn1sxR03UyxU+ey4AdKe9LG3TJVnB0WcgxpobpbqweLH
yzcEQCNoFV3r1rlE13Y0aE31/9apoEwiYvqAzEmE38TukDLl/Qg8rkR1t0/lok2P
G6pWqdN7pmoUovBTvDi5YOthcjZcdOTXXn2Yw4RZVF9uhRsVfku1Eg25SnOje3uY
smtQLME4eESbePdjyV/okCIle66uHZse+7gNyNmWpf01hM+VmAySIAyKa0Ku8AXZ
MydEcJTebrNfW9uMLsBx3Ts7z/CBfRng6F8louJGlZtlSwddTkZVcb26T20xeo0a
ZvdFXM2djTi/a5nbBoZQL85AEeV7HaphFLdPrgmMtS8sSZUEVvdaxp7WJsVuF9cO
Nxsvx40OYTvfco0W41Lm8/sEuQ7YueEVpZxiv5kX56GTU9vXaOOi+8Z7Ee2w6Adz
4hrGZkzztggs4tM9geNYnd0XCdZ/ICAskKJABg7biDD1PhEBrqCIqSE3U497vibQ
Mpkkl/Zpp0BirhGWNyTg8K4JrsQ=
=d320
-----END PGP PUBLIC KEY BLOCK-----
EOT

rpm --import /root/RPM-GPG-KEY-azulsystems
rpm --rebuilddb

# Do not include "gpgkey" in the repo definition -- we've already imported it, above.
cat <<'EOT' > /etc/yum.repos.d/zulu.repo
[zulu]
name=zulu-$releasever - Azul Systems Inc., Zulu packages for $basearch
baseurl=http://repos.azulsystems.com/rhel/$releasever/$basearch
enabled=1
gpgcheck=1
protect=1
EOT
