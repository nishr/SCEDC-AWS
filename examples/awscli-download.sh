#!/bin/bash
mkdir ~/data
mkdir ~/data/2016
mkdir ~/data/2016/2016_186
aws s3 sync s3://scedc-pds/2016/2016_186 data/2016/2016_186 --exclude "*" --include "*.ms"
