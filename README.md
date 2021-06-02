## Spring Boot + Spark + Redis

#### [Goals]
* Spring Boot 에서 Spark 연동
* Bigquery, Mysql 결과를 Spark 에서 연산 가능하도록 함
 
#### [Issue]
* 지속적인 호출 시 매번 쿼리 호출
* Spark 연동시 json 파일 처리
* cloud 비용 최소화
* 반복 연산 최소화
* 1,000,000 row 데이터 처리시 메모리 문제

#### [Issue 처리]
* 쿼리 결과 로컬 temp directory 에 저장
  * cloud 사용 시 비용 문제 및 expire time setting 이 어려움

* Redis 활용
    * Redis 에 쿼리 id 를 key 로 넣고 expire time 지정
    * expire 시에 event 생성
    * event 에서 key 값을 message 로 받아 json 파일 삭제

* 1,000,000 row json parsing 시 연속 처리시 메모리 이슈
  1. bigquery 결과 row 를 json 형태로 받은 후 File 에 한줄씩 쓰기 - paring 하지 않음
  2. google cloud storage 에 저장 후 File download


#### [사용 Dependencies]
[Gradle](build.gradle)
* Spring
* thymeleaf
* Mysql
* JPA
* Lombok
* Logging
* Jackson
* Redis
* Spark
* GCP - Bigquery

#### [Function]
* 결과 값을 json 형태로 변형
* json 값을 파일로 temp 폴더에 저장 
* redis 에 저장된 key 생성 후 expire time 생성
* redis expire time 이 되었을 시 message event 를 통해서 json 파일 삭제
* json 파일에서 데이터를 읽어와 spark 연동 해서 연산 처리

[Bigquery Spark 연동](./src/main/java/com/test/spark/controller/test/BigQueryController.java)

[단순 Json 값 Spark](./src/main/java/com/test/spark/controller/test/JsonController.java)

[단순 HashMap List 값 Spark](./src/main/java/com/test/spark/controller/test/HashMapController.java)