#! /bin/sh
demos="\
    project_demo04-SimpleSelect
    project_demo01-TimeSeriesEnrich
    project_demo02-FraudDetection
    project_demo03-GreenTrip
    project_demo00-SecOps"
for d in $demos; do
    (cd demo/$d && python3 run.py --dbsp_url http://dbsp:8080 --actions create)
done
for d in $demos; do
    (cd demo/$d && python3 run.py --dbsp_url http://dbsp:8080 --actions compile)
done
