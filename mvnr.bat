SET /P _go= Build and run on Spark?
IF "%_go%"=="y" GOTO :build

GOTO :end

:build
echo mvn
call mvn clean install

SET /P _success= Build successful?
IF "%_success%"=="y" GOTO :run

GOTO :end

:run

echo copying assembly jar
copy   C:\Users\jake\__workspace\scalaProjects\scalaML\target\scalaML-1.0-SNAPSHOT-jar-with-dependencies.jar C:\Users\jake\_servers\spark-2.2.1-bin-hadoop2.7\ikoda\scalaML-1.0-SNAPSHOT-jar-with-dependencies.jar
copy   C:\Users\jake\__workspace\scalaProjects\scalaML\pipeline.bat C:\Users\jake\_servers\spark-2.2.1-bin-hadoop2.7\pipeline.bat

echo calling pipeline
cd C:\Users\jake\_servers\spark-2.2.1-bin-hadoop2.7



call pipeline.bat
cd C:\Users\jake\__workspace\scalaProjects\scalaML
:end