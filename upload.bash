#!/bin/bash
pushd data > /dev/null

LISTING=""
for line in $(find -type f -name '*.csv.gz' -not -path '*/local/*')
do
    gsutil -h "Content-Encoding:gzip" -h "Content-Type:text/plain; charset=utf-8" \
        cp "$line" "gs://data.pickban.win/${line:2:-3}"
    LISTING="${LISTING}\n${line:2:-3}"
done

popd > /dev/null
