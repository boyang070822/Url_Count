baseDir=`dirname $0`/..
echo $0,$baseDir
startTime=$1
endTime=$2
CountUidUrlJar=$baseDir/CountUidUrl/target/Count_UidUrl-jar-with-dependencies.jar
CountUidUrlMain=com.elex.bigdata.countuidurl.CountUidUrl
output=/user/hadoop/url_count/${startTime}_${endTime}
echo "hadoop jar $CountUidUrlJar $CountUidUrlMain $output $startTime $endTime"
logFile=/data/log/user_category/processLog/CountUidUrl.log
hadoop jar $CountUidUrlJar $CountUidUrlMain $output s$startTime e$endTime >> $logFile 2>&1
