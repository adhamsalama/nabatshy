FROM node:20-alpine AS ui
WORKDIR /app
COPY ui/package*.json ui/
RUN npm install --prefix ui
COPY ui ./ui
COPY docs/assets ./docs/assets
RUN npm run build --prefix ui

FROM golang:1.24-bookworm AS backend
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . ./
COPY --from=ui /app/ui/dist ./ui/dist
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o nabatshy .

FROM gcr.io/distroless/base-debian12
WORKDIR /app
COPY --from=backend /app/nabatshy .
CMD ["./nabatshy"]
