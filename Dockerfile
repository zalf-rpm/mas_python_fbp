FROM ghcr.io/prefix-dev/pixi:0.70.1 AS build-base

WORKDIR /app

COPY pyproject.toml pixi.lock README.md ./
COPY zalfmas_fbp ./zalfmas_fbp

RUN pixi global install git

FROM build-base AS prod-build
RUN pixi install --locked --environment default
RUN pixi shell-hook --environment default > /shell-hook.sh
RUN echo 'exec "$@"' >> /shell-hook.sh
COPY binaries /app/binaries
COPY configs /app/configs

FROM build-base AS dev-build
RUN pixi install --locked --environment dev
RUN pixi shell-hook --environment dev > /shell-hook.sh
RUN echo 'exec "$@"' >> /shell-hook.sh
COPY binaries /app/binaries
COPY configs /app/configs

FROM debian:13 AS runtime

WORKDIR /app

RUN useradd -m -s /bin/bash appuser

RUN mkdir -p /app/outputs && chown appuser:appuser /app/outputs

EXPOSE 8000

USER appuser

FROM runtime AS prod
COPY --from=prod-build /app/.pixi/envs/default /app/.pixi/envs/default
COPY --from=prod-build /shell-hook.sh /shell-hook.sh
COPY --from=prod-build /app/binaries /app/binaries
COPY --from=prod-build --chown=appuser:appuser /app/configs /app/configs
COPY --from=prod-build --chown=appuser:appuser /app/zalfmas_fbp /app/zalfmas_fbp
ENTRYPOINT ["/bin/bash", "/shell-hook.sh"]

FROM runtime AS dev
COPY --from=dev-build /app/.pixi/envs/dev /app/.pixi/envs/dev
COPY --from=dev-build /shell-hook.sh /shell-hook.sh
COPY --from=dev-build /app/binaries /app/binaries
COPY --from=dev-build --chown=appuser:appuser /app/configs /app/configs
COPY --from=dev-build --chown=appuser:appuser /app/zalfmas_fbp /app/zalfmas_fbp
ENTRYPOINT ["/bin/bash", "/shell-hook.sh"]
