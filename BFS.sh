set -e
#set username, passwd and database name
USERNAME="peng"
PASSWARD="1234"
DATABASENAME="mydb4"
DST="130"
STIME="1562734588971"

DATABASEADDRESS="jdbc:postgresql://localhost:5432/${DATABASENAME}"
TESTDATABASENAME="test${DATABASENAME}"
TESTDATABASEADDRESS="jdbc:postgresql://localhost:5432/${TESTDATABASENAME}"
COMDBNAME="compressed${DATABASENAME}"
COMDATABASEADDRESS="jdbc:postgresql://localhost:5432/${COMDBNAME}"


javac DataCompress/*.java

java -classpath DataCompress/postgresql-42.2.6.jar:./ DataCompress.BFS $USERNAME $PASSWARD $DST $STIME $TESTDATABASENAME $TESTDATABASEADDRESS original $DATABASENAME

java -classpath DataCompress/postgresql-42.2.6.jar:./ DataCompress.BFS $USERNAME $PASSWARD $DST $STIME $COMDBNAME $COMDATABASEADDRESS compress $DATABASENAME

#java check.java "nodeResultcompresseddb" "nodeResult${TESTDATABASENAME}"