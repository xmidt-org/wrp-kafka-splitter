#!/bin/bash
# SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
# SPDX-License-Identifier: Apache-2.0

# Pre-install script for splitter package

set -e

# Create splitter user and group if they don't exist
if ! id "splitter" &>/dev/null; then
    echo "Creating splitter user and group..."
    groupadd -r splitter || true
    useradd -r -g splitter -s /bin/false -d /nonexistent -m splitter || true
fi

exit 0
