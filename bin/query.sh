baseDir=`dirname $0`/..
echo $0,$baseDir
QueryJar=$baseDir/QueryUidUrl/target/Query_UidUrl-jar-with-dependencies.jar
QueryMain=com.elex.bigdata.queryuidurl.QueryUidUrl

echo "java -cp $QueryJar $QueryMain $1 $2"
java -cp $QueryJar $QueryMain $1 $2
