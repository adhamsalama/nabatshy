# Stage 1: Build UI (Vite/React/TS) on Alpine
FROM node:20-alpine AS ui
WORKDIR /app/ui


COPY ui/package*.json ./
RUN npm install

COPY ui ./
RUN npm run build

# Stage 2: Build Go backend on Alpine
FROM golang:1.23-alpine AS backend
WORKDIR /app


COPY go.mod go.sum ./
RUN go mod download

COPY . ./
COPY --from=ui /app/ui/dist ./ui/dist

RUN go build -o nabatshy .

# Stage 3: Minimal Alpine runtime
FROM alpine:3.18

WORKDIR /app
COPY --from=backend /app/nabatshy .

CMD ["./nabatshy"]
