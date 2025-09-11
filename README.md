# Docker image

The Docker image is able to execute the runnable components of this repository.
For example, the channel_starter_service.

Run with

```bash
docker run -it zalfrpm/mas_python_fbp python -m zalfmas_fbp.run.channel_starter_service
```

Expose port 9989 and mount a local config to ./configs/channel_starter_service.toml:

```bash
docker run -it \
  -p 9989:9989 \
  -v "$(pwd)/configs/channel_starter_service.toml:/app/configs/channel_starter_service.toml:ro" \
  zalfrpm/mas_python_fbp \
  python -m zalfmas_fbp.run.channel_starter_service
```

Or the local components service 

```bash
docker run -it zalfrpm/mas_python_fbp python -m zalfmas_fbp.run.local_components_service
```
