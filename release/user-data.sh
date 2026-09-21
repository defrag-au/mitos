#!/bin/sh
# First-boot user-data for an unattended mitos node. Keep this ≤10 lines.
# PIN a release; never "latest". Source-mode boxes skip the curl and drop
# the binary at /usr/local/bin/mitos themselves.
set -eu
VERSION="${MITOS_RELEASE:-UNRELEASED}"
curl -fsSLO "https://github.com/defrag-au/mitos/releases/download/${VERSION}/mitos-$(uname -m).tar.gz"
tar -C /usr/local/bin -xzf "mitos-$(uname -m).tar.gz"
mitos init --network mainnet --profile full
systemctl enable --now mitos-bootstrap mitos
