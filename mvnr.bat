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

echo copying docs

xcopy C:\Users\jake\__workspace\scalaProjects\ikodaML\target\site\scaladocs C:\Users\jake\__workspace\scalaProjects\ikodaML\docs\scaladoc /S /E /F /R /Y /I


:end