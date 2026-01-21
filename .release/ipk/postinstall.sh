#!/bin/bash
# SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
# SPDX-License-Identifier: Apache-2.0

# Post-install script for splitter package

set -e

# Set permissions on configuration directory
if [ -d /etc/splitter ]; then
    chown -R splitter:splitter /etc/splitter || true
    chmod 755 /etc/splitter || true
fi

# Enable and start the systemd service
if command -v systemctl &>/dev/null; then
    systemctl daemon-reload || true
    systemctl enable splitter || true
    echo "To start the service, run: systemctl start splitter"
fi

exit 0
