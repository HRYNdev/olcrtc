# Docker Local Setup

This guide shows one way to run OLCRTC with a local-only Docker setup.

## Main idea
 
- keep the editable Docker files in a hidden `.local` folder
- keep config files out of Git in the `.local` folder 
- allow users to update repository normally with `git pull`

## 1. Clone the repository

```bash
git clone https://github.com/openlibrecommunity/olcrtc.git
cd olcrtc
```

## 2. Update to the latest version

When you want to get a newer version from upstream, run:

```bash
git pull https://github.com/openlibrecommunity/olcrtc.git
```

If you use submodules in your environment, you can keep the same pull flow and add `--recurse-submodules`.

## 3. Create the local folder

Create a `.local` directory in the repository root:

```bash
mkdir -p .local
```

This folder should contain files that belong only to your machine.

## 4. Copy the server compose file into `.local`

Copy the server compose file so your local version does not get overwritten by the next pull:

```bash
cp docker-compose.server.yml .local/docker-compose.server.yml
```

If the upstream compose file changes later, copy it again after pulling updates.

## 5. Create the local env file

Create `.local/.env` and fill in the runtime values according to the connection type of your choice.

An example can be found in `docs/examples/.env.telemost.server.example`.

## 6. Start OLCRTC

Run Docker Compose with the local compose file and env file:

```bash
docker compose -f .local/docker-compose.server.yml --env-file .local/.env up -d
```

Check the container status:

```bash
docker compose -f .local/docker-compose.server.yml --env-file .local/.env ps
```

Follow the logs if you need to debug startup:

```bash
docker compose -f .local/docker-compose.server.yml --env-file .local/.env logs -f
```

## 7. Update the local setup later

After a new upstream pull, copy the current server compose file again:

```bash
git pull https://github.com/openlibrecommunity/olcrtc.git
cp docker-compose.server.yml .local/docker-compose.server.yml
```

Then restart the container with the same command:

```bash
docker compose -f .local/docker-compose.server.yml --env-file .local/.env.telemost.server up -d
```

## Notes

- Keep all local Docker files inside `.local`.
- Do not commit `.local` to the repository.
- Keep shared documentation in `docs/` and server-specific values in `.local`.
- If you change the upstream compose file, refresh the local copy before starting the container again.
