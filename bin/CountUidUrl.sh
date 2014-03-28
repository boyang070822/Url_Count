baseDir=`dirname $0`/..
echo $0,$baseDir
startTime=$1
endTime=$2
CountUidUrlJar=$baseDir/CountUidUrl/target/Count_UidUrl-jar-with-dependencies.jar
CountUidUrlMain=com.elex.bigdata.countuidurl.CountUidUrl
outputBase=/user/hadoop/url_count
echo "hadoop jar $CountUidUrlJar $CountUidUrlMain $outputBase $startTime $endTime"
logFile=/data/log/user_category/processLog/CountUidUrl.log
hadoop jar $CountUidUrlJar $CountUidUrlMain $outputBase s$startTime e$endTime >> $logFile 2>&1
