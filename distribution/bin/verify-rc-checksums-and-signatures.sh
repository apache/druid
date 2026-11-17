#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Downloads a Druid release candidate's src/bin tarballs from dist/dev SVN and verifies
# their sha512 checksums and GPG signatures.

set -uo pipefail

usage() {
  cat <<EOF
Usage: $0 --rc <rc> --src-sha512 <hex> --bin-sha512 <hex> [options]

Required:
  --rc <rc>              Release candidate, e.g. 39.0.0-rc1
  --src-sha512 <hex>     The "src:" checksum line from the [VOTE] email
  --bin-sha512 <hex>     The "bin:" checksum line from the [VOTE] email

Options:
  --signer-fingerprint <hex>   Fingerprint from the email's "signed with the following key" line;
                                asserted against the signature's actual signing key if given.
  --workdir <dir>              Download directory (default: a fresh mktemp dir; reused if it
                                already holds the files, so re-running after a fix is cheap).
  --cleanup                     Remove --workdir once all checks pass. Left in place on any FAIL,
                                so the downloaded files are still there to inspect.
  -h, --help                    Show this help.
EOF
}

rc="" src_sha512="" bin_sha512="" signer_fingerprint="" workdir="" cleanup=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rc) rc="$2"; shift 2 ;;
    --src-sha512) src_sha512="$2"; shift 2 ;;
    --bin-sha512) bin_sha512="$2"; shift 2 ;;
    --signer-fingerprint) signer_fingerprint="$2"; shift 2 ;;
    --workdir) workdir="$2"; shift 2 ;;
    --cleanup) cleanup=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "$rc" || -z "$src_sha512" || -z "$bin_sha512" ]]; then
  usage
  exit 1
fi

version="${rc%-rc*}"
workdir="${workdir:-$(mktemp -d -t "druid-verify-${rc}")}"
base_url="https://dist.apache.org/repos/dist/dev/druid/${rc}"

mkdir -p "$workdir"
cd "$workdir"
echo "Working in $workdir"

fetch() {
  local name="$1"
  [[ -f "$name" ]] && return 0
  echo "Fetching $name"
  if command -v svn >/dev/null 2>&1; then
    svn export -q "${base_url}/${name}" "${name}"
  else
    curl -sfL -o "${name}" "${base_url}/${name}"
  fi
}

for kind in src bin; do
  file="apache-druid-${version}-${kind}.tar.gz"
  fetch "$file"
  fetch "${file}.asc"
  fetch "${file}.sha512"
done

fail=0

echo
echo "== Checksums =="
for kind in src bin; do
  file="apache-druid-${version}-${kind}.tar.gz"
  computed=$(shasum -a512 "$file" | cut -d ' ' -f1)
  from_file=$(tr -d '[:space:]' <"${file}.sha512")

  if [[ "$computed" == "$from_file" ]]; then
    echo "OK:   $file matches its own .sha512 file"
  else
    echo "FAIL: $file does NOT match its own .sha512 file (download may be corrupt)"
    fail=1
  fi

  expected_var="${kind}_sha512"
  expected="${!expected_var}"
  if [[ "$from_file" == "$expected" ]]; then
    echo "OK:   $file .sha512 matches the vote email's ${kind}: line"
  else
    echo "FAIL: $file .sha512 does NOT match the vote email's ${kind}: line"
    echo "      .sha512 file: $from_file"
    echo "      vote email:   $expected"
    echo "      -> the file on SVN was swapped after the email was sent; this is a -1"
    fail=1
  fi
done

echo
echo "== Signatures =="
curl -s https://dist.apache.org/repos/dist/release/druid/KEYS | gpg --import >/dev/null 2>&1

for kind in src bin; do
  file="apache-druid-${version}-${kind}.tar.gz"
  out=$(gpg --verify "${file}.asc" "$file" 2>&1)
  if ! echo "$out" | grep -q "Good signature"; then
    echo "FAIL: $file signature did not verify"
    echo "$out" | sed 's/^/      /'
    fail=1
    continue
  fi

  signing_fp=$(echo "$out" | grep "Primary key fingerprint" | sed -E 's/.*fingerprint: *//; s/ //g')
  if [[ -n "$signer_fingerprint" ]]; then
    normalized_expected=$(echo "$signer_fingerprint" | tr -d ' ')
    if [[ "$signing_fp" == "$normalized_expected" ]]; then
      echo "OK:   $file has a good signature from $signing_fp (matches email)"
    else
      echo "FAIL: $file has a good signature, but from $signing_fp, not the email's $normalized_expected"
      fail=1
    fi
  else
    echo "OK:   $file has a good signature from $signing_fp (not cross-checked: no --signer-fingerprint given)"
  fi
done

echo
if [[ "$fail" -eq 0 ]]; then
  echo "All checksum and signature checks passed."
  if [[ "$cleanup" -eq 1 ]]; then
    cd /
    rm -rf "$workdir"
    echo "Removed $workdir (--cleanup)."
  fi
else
  echo "One or more checks FAILED, see above."
  [[ "$cleanup" -eq 1 ]] && echo "Not removing $workdir despite --cleanup: left in place for inspection."
fi
exit "$fail"
