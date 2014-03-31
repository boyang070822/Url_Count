startTime=$1
endTime=$2
sh CountUidUrl.sh $startTime $endTime
sh ../LabeledDocuments/bin/getLabeledDocs.sh ${startTime:0:8} ${endTime:0:8}
