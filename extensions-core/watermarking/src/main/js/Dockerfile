FROM node:boron AS buildimage
WORKDIR /app
COPY . /app
RUN npm install && npm run build
FROM nginx:1.13.11-alpine-perl
ENV WATERMARK_API_PORT=8082
ENV WATERMARK_API_HOST=docker.for.mac.host.internal
COPY --from=buildimage /app/public /usr/share/nginx/html/console
COPY --from=buildimage /app/nginx.default.conf /etc/nginx/conf.d/default.conf
COPY --from=buildimage /app/nginx.conf /etc/nginx/nginx.conf
COPY --from=buildimage /app/public/favicon /usr/share/nginx/html
