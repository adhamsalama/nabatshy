FROM node:20-alpine AS ui
WORKDIR /app/ui
COPY ui/package*.json ./
RUN npm install
COPY ui ./
RUN npm run build

FROM golang:1.23-bookworm AS backend
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . ./
COPY --from=ui /app/ui/dist ./ui/dist
RUN CGO_ENABLED=1 GOOS=linux go build -o nabatshy .

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=backend /app/nabatshy .
CMD ["./nabatshy"]
