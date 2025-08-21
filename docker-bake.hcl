group "default" {
  targets = ["mas_python_fbp"]
}

variable "TAG" {
  default = "latest"
}

target "mas_python_fbp" {
  context    = "."
  dockerfile = "Dockerfile"
  tags       = ["zalfrpm/mas_python_fbp:${TAG}", "zalfrpm/mas_python_fbp:latest"]
  target     = "prod"
  annotations = [
    "org.opencontainers.image.title=mas_python_fbp",
    "org.opencontainers.image.description=Utilities for the ZALF MAS Python FBP project",
    "org.opencontainers.image.url=https://hub.docker.com/r/zalfrpm/mas_python_fbp",
    "org.opencontainers.image.source=https://github.com/zalf-rpm/mas_python_fbp",
    "org.opencontainers.image.documentation=https://github.com/zalf-rpm/mas_python_fbp/blob/docker/README.md"
  ]
}
