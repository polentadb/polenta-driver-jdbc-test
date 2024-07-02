FROM openjdk:8

RUN mkdir -p /var/polenta-db/libs

COPY polenta-db-0.0.3.jar /var/polenta-db/libs/.

CMD ["java", "-jar", "/var/polenta-db/libs/polenta-db-0.0.3.jar"]