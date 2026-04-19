FROM eclipse-temurin:17-jdk
WORKDIR /app
COPY target/kafka-producer-1.0.0.jar kafka-producer-1.0.0.jar
ENTRYPOINT ["java", "-jar", "kafka-producer-1.0.0.jar"]