#!/usr/bin/env bash
# Install the pinned Linux AMD64 kind binary used by Kubernetes CI.
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: bash scripts/install-kind.sh DESTINATION" >&2
  exit 2
fi

kind_version=v0.33.0
kind_asset_id=531396433
kind_sha256=aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d
kind_destination=$1
mkdir -p "$(dirname "$kind_destination")"
kind_download=$(mktemp "${kind_destination}.XXXXXX")
trap 'rm -f "$kind_download"' EXIT

curl_options=(--fail --silent --show-error --location --proto '=https' --proto-redir '=https'
  --connect-timeout 10 --max-time 30 --retry 2 --retry-delay 5 --retry-max-time 60)
if ! curl "${curl_options[@]}" --output "$kind_download" \
  "https://github.com/kubernetes-sigs/kind/releases/download/$kind_version/kind-linux-amd64"; then
  # The official asset API bypasses failures on GitHub's browser download route.
  echo "kind release download failed; trying the official GitHub asset API" >&2
  curl "${curl_options[@]}" --header 'Accept: application/octet-stream' \
    --output "$kind_download" \
    "https://api.github.com/repos/kubernetes-sigs/kind/releases/assets/$kind_asset_id"
fi

# Never expose partial downloads or unchecked fallback content as an executable.
echo "$kind_sha256  $kind_download" | sha256sum --check
chmod 755 "$kind_download"
mv -f "$kind_download" "$kind_destination"
