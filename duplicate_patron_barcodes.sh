#!/bin/bash
cd "$(dirname "$0")"
echo "Start time: $(date)" >> log.txt
./venv/bin/python duplicate_patron_barcodes.py >> log.txt &
wait
