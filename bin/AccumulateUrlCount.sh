baseDir=`dirname $0`/..
echo $0,$baseDir
startTime=$1
endTime=$2
CountUidUrlJar=$baseDir/CountUidUrl/target/Count_UidUrl-jar-with-dependencies.jar
CountUidUrlMain=com.elex.bigdata.urlcount.Accumulate
outputBase=/user/hadoop/url_count/
echo "hadoop jar $CountUidUrlJar $CountUidUrlMain -outputBase $outputBase -startTime $startTime -endTime $endTime >> $logFile 2>&1"
logFile=/data/user_category/llda/logs/AccumulateUidUrl.log
hadoop jar $CountUidUrlJar $CountUidUrlMain -outputBase $outputBase -startTime $startTime -endTime $endTime >> $logFile 2>&1
