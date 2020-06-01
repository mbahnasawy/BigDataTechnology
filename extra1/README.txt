docker run --hostname=quickstart.cloudera --privileged=true -t -i -v /Users/datvt/Work/cloudera:/src -p 8888:8888 -p 80:80 -p 7180:7180 cloudera/quickstart /usr/bin/docker-quickstart

hdfs dfs -mkdir /user/cloudera/input

hdfs dfs -put /src/input/NCDC-Weather_small.txt /user/cloudera/input

hadoop jar /src/bdt_extra1.jar bdt_extra1.Runner /user/cloudera/input /user/cloudera/output

# merge to one file name StationTempRecord in outside dir
hdfs dfs -getmerge /user/cloudera/output /src/input/StationTempRecord

# put five StationTempRecord to output folder in cloudera, result as in image
hdfs dfs -put /src/input/StationTempRecord /user/cloudera/output/StationTempRecord