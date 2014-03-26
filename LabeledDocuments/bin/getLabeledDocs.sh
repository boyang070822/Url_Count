baseDir=`dirname $0`/..
echo $0,$baseDir
inputDir=/user/hadoop/url_count/$1
outputDir=/user/hadoop/url_count/labeledDocs/$2
localOutputFile=/data/log/user_category/llda/labeledDocs.$2
Jar=$baseDir/target/LabeledDocuments-jar-with-dependencies.jar
Main=com.elex.bigdata.labeledDocuments.LabeledDocument
echo "hadoop fs -rm -r $outputDir"
hadoop fs -rm -r $outputDir
echo "hadoop jar $Jar $Main $inputDir $outputDir"
hadoop jar $Jar $Main $inputDir $outputDir
echo "hadoop fs -getmerge $outputDir $localOutputFile"
hadoop fs -getmerge $outputDir $localOutputFile
