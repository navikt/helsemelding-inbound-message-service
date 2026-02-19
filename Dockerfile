FROM gcr.io/distroless/java21-debian12@sha256:f995f26f78b65251a0511109b07401a5c6e4d7b5284ae73e8d6577d24ff26763

COPY build/libs/app.jar /app/app.jar

WORKDIR /app
ENV TZ="Europe/Oslo"
EXPOSE 8080
USER nonroot
CMD [ "app.jar" ]