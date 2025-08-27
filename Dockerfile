FROM ghcr.io/prefix-dev/pixi:latest AS build

# copy source code, pixi.toml and pixi.lock to the container
COPY . /app
WORKDIR /app
# install git so pixi can install the dependencies
RUN pixi global install git
# assumes that you have a `prod` environment defined in your pixi.toml
RUN pixi install
# Create the shell-hook bash script to activate the environment
RUN pixi shell-hook > /shell-hook.sh

# extend the shell-hook script to run the command passed to the container
RUN echo 'exec "$@"' >> /shell-hook.sh

FROM debian:13 AS prod

WORKDIR /app

RUN useradd -m -s /bin/bash appuser

# only copy the production environment into prod container
# please note that the "prefix" (path) needs to stay the same as in the build container
COPY --from=build /app/.pixi/envs/default /app/.pixi/envs/default
COPY --from=build /shell-hook.sh /shell-hook.sh
COPY --from=build /app/binaries /app/binaries
COPY --from=build /app/configs /app/configs
COPY --from=build /app/zalfmas_fbp /app/zalfmas_fbp

EXPOSE 8000

USER appuser

# set the entrypoint to the shell-hook script (activate the environment and run the command)
# no more pixi needed in the prod container
ENTRYPOINT ["/bin/bash", "/shell-hook.sh"]

