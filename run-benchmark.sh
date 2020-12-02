#!/bin/sh

rm -fr target
rm -fr benchmark/target

# sbt 'benchmark/jmh:run -prof jmh.extras.JFR:dir=.. -i 5 -wi 3 -f1 -t1'
sbt ";clean ;benchmark/jmh:compile ;benchmark/jmh:run -i 15 -wi 15 -f1 -t1 $@"
