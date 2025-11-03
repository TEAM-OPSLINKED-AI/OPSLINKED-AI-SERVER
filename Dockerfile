FROM eclipse-temurin:17-jdk AS builder
WORKDIR /app

# Gradle Wrapper & 설정 파일 복사
COPY gradlew .
COPY gradle gradle
COPY build.gradle .
COPY settings.gradle .
COPY src src

# 권한 부여 및 빌드
RUN chmod +x gradlew
RUN ./gradlew clean bootJar -x test

FROM eclipse-temurin:17-jdk
WORKDIR /app

# 빌드된 jar 복사
COPY --from=builder /app/build/libs/*.jar app.jar

# 포트 노출
EXPOSE 8080

# 애플리케이션 실행
ENTRYPOINT ["java", "-jar", "app.jar"]
