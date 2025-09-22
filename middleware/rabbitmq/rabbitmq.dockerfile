FROM rabbitmq:3.9.16-management-alpine
RUN apk update && apk add curl