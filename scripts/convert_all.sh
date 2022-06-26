#!/bin/sh
#
# Converts all images in data/downloaded to data/8bit
#
set -e

../py8bit/scripts/run.sh -p 20 -d data/8bit --skip_existing true --continue_after_errors true --images data/downloaded/*.jpg