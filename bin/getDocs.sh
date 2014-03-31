startTime=$1
endTime=$2
baseDir=`dirname $0`/..
sh $baseDir/bin/CountUidUrl.sh $startTime $endTime
sh $baseDir/LabeledDocuments/bin/getLabeledDocs.sh ${startTime}_${endTime} ${startTime}_${endTime}
