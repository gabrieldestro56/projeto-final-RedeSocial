FROM node:18-slim
RUN npm install -g http-server
WORKDIR /app
COPY client/ .
CMD ["http-server", "-p", "8080"]