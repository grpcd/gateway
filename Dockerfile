ARG build_dir=/go/src
ARG artifact_name=app
ARG health_name=grpc_health_probe
# ARG grpc_health_probe_version=v0.4.57
ARG module_path=.

FROM golang:alpine AS deps
ARG module_path=.
ARG build_dir
WORKDIR ${build_dir}
COPY ${module_path}/go.mod ${module_path}/go.sum ./
RUN go mod download


FROM deps AS build
ARG artifact_name
COPY . .
RUN CGO_ENABLED=0 go build -v -ldflags="-w -s" -o ${artifact_name} ${module_path}


# FROM ghcr.io/grpc-ecosystem/grpc-health-probe:${grpc_health_probe_version} AS grpc-health-probe


FROM scratch AS run
ARG build_dir
ARG artifact_name
ARG grpc_health_probe_version

COPY --from=build ${build_dir}/${artifact_name} /app
# COPY --from=grpc-health-probe /ko-app/grpc-health-probe /health

CMD ["/app"]
