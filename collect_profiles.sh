#!/usr/bin/env bash

until ./collect_profiles.py; do
    echo "Restarting..."
done