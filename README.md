## Build and run
```bash
./gradlew fatJar
java -jar build/libs/AWSDynamoDB-fat-1.0-SNAPSHOT.jar path/to/file.csv
```
After that full table will be printed, and you can run select sql queries (name of table is 'main'), and finally type 'exit' to finish:
```bash
java -jar build/libs/AWSDynamoDB-fat-1.0-SNAPSHOT.jar path/to/file.csv
1,2,3
2,3,4
1,2,3
select * from main where a = 1
1,2,3
1,2,3
exit
```
