FROM openjdk:11
COPY target/scala-2.13/ContribsGH2-Z-assembly-0.1.jar contribsgh2-z.jar
ENTRYPOINT ["java","-jar","/contribsgh2-z.jar"]
