echo Copying custom JS and JSX files to the runtime folder
cp -a -v ../src/ ../app/
cp -a -v ../src/lambda/src/index.js ../app/lambda/src/
cp -a -v ../src/lambda/src/mysql-credentials.json ../app/lambda/src/
