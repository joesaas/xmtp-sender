FROM node:24-slim

WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev

COPY src ./src

ENV PORT=3000
EXPOSE 3000

# XMTP gRPC keepalive tuning (see docs/DESIGN.md §1.4)
ENV XMTP_GRPC_KEEPALIVE_INTERVAL_SECS=45
ENV XMTP_GRPC_KEEPALIVE_TIMEOUT_SECS=20

CMD ["node", "src/index.js"]
