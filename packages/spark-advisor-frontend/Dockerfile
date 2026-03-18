FROM node:22-alpine AS build

WORKDIR /app
COPY packages/spark-advisor-frontend/package.json packages/spark-advisor-frontend/package-lock.json ./
RUN npm ci
COPY packages/spark-advisor-frontend/ ./
RUN npm run build

FROM nginx:1.27-alpine

COPY --from=build /app/dist/ /usr/share/nginx/html/

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
