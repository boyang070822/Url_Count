baseDir=`dirname $0`/..
echo $0,$baseDir
refTime=$1
currentTime=$2
inputDir=/user/hadoop/url_count
outputDir=/user/hadoop/user_category/llda/docs
localOutputFile=/data/user_category/llda/docs
Jar=$baseDir/target/LabeledDocuments-jar-with-dependencies.jar
Main=com.elex.bigdata.labeledDocuments.getDocs.GetDocs
logFile=/data/user_category/llda/logs/getLabeledDocs.log
echo "hadoop jar $Jar $Main -inputBase $inputDir -outputBase $outputDir -refInputTime $refTime -inputTime $currentTime -outputTime $currentTime  >> $logFile 2>&1"
hadoop jar $Jar $Main -inputBase $inputDir -outputBase $outputDir -refInputTime $refTime -inputTime $currentTime -outputTime $currentTime  >> $logFile 2>&1