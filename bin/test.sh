baseDir=`dirname $0`/..
echo $0,$baseDir
day=$1
CountUidUrlJar=$baseDir/CountUidUrl/target/Count_UidUrl-jar-with-dependencies.jar
CountUidUrlMain=com.elex.bigdata.countuidurl.CountUidUrl
CountGlobalUrlJar=$baseDir/CountGlobalUrl/target/Count_GlobalUrl-jar-with-dependencies.jar
CountGlobalUrlMain=com.elex.bigdata.countglobalurl.CountGlobalUrl


uidUrlDir=/user/hadoop/url_count/$day
globalUrlDir=/user/hadoop/url_count/global/test/$day
localUrlFrequentFile=/data/log/user_category/click_log_file

echo "hadoop fs -rm -r $uidUrlDir"
hadoop fs -rm -r $uidUrlDir

echo "hadoop jar $CountUidUrlJar $CountUidUrlMain  $uidUrlDir s${day}000000"
hadoop jar $CountUidUrlJar $CountUidUrlMain  $uidUrlDir s${day}000000

echo "hadoop fs -rm -r $globalUrlDir"
hadoop fs -rm -r $globalUrlDir

echo "hadoop jar $CountGlobalUrlJar $CountGlobalUrlMain $uidUrlDir $globalUrlDir"
hadoop jar $CountGlobalUrlJar $CountGlobalUrlMain $uidUrlDir $globalUrlDir

echo "hadoop fs -getmerge $globalUrlDir $urlFrequentFile"
hadoop fs -getmerge $globalUrlDir $localUrlFrequentFile

LoadUidUrlJar=$baseDir/LoadUidUrl/target/Load_UidUrl-jar-with-dependencies.jar
LoadUidUrlMain=com.elex.bigdata.loaduidurl.LoadUidUrlCount

uidUrlCountTable=uidUrlCount_22find

echo "hadoop jar $LoadUidUrlJar $LoadUidUrlMain $uidUrlDir $uidUrlCountTable s$day"
hadoop jar $LoadUidUrlJar $LoadUidUrlMain $uidUrlDir $uidUrlCountTable s${day}000000

parentBaseDir=$baseDir/..
TrainJar=$parentBaseDir/user_category/train/target/user_category_train-jar-with-dependencies.jar
TrainMain=com.elex.bigdata.user_category.train.BayesTrainer
click_log_file=$localUrlFrequentFile
url_category_file=/data/log/user_category/mirrors/url_category
type=$2
echo "java -cp $TrainJar $TrainMain $click_log_file $url_category_file $type"
java -cp $TrainJar $TrainMain $click_log_file $url_category_file $type




