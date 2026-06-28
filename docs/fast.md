<div align="center">

<img src="https://github.com/openlibrecommunity/material/blob/master/olcrtc.png" width="250" height="250">

![License](https://img.shields.io/badge/license-WTFPL-0D1117?style=flat-square&logo=open-source-initiative&logoColor=green&labelColor=0D1117)
![Golang](https://img.shields.io/badge/-Golang-0D1117?style=flat-square&logo=go&logoColor=00A7D0)

[RU](fast.ru.md) / **EN**

</div>

# Quick start

> **Important:** always check that the video call service you need is on the allow lists, that it works in your network, and so on. If not, use another one.

This method runs `olcrtc` as an ordinary native binary. You need Go 1.26+, mage, git and curl.

## Install dependencies

```sh
apt install git curl        # Debian / Ubuntu / Mint
pacman -S git curl          # Arch / CachyOS / Manjaro
dnf install git curl        # Fedora / RHEL / CentOS
```

Install Go 1.26+ and mage:

```sh
go install github.com/magefile/mage@latest
echo 'export PATH="$HOME/go/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

If the machine has less than 4 GB RAM, enable swap before building:

```sh
sudo fallocate -l 4G /swapfile && sudo chmod 600 /swapfile && sudo mkswap /swapfile && sudo swapon /swapfile
```

## Build

```sh
git clone https://github.com/openlibrecommunity/olcrtc --recurse-submodules
cd olcrtc
mage build
```

The binary lands in `build/`, for example:

```sh
./build/olcrtc-linux-amd64
```

## Generate a key

The key must match on the server and the client.

```sh
openssl rand -hex 32
```

## Run the server

Create `server.yaml`:

> **Important:** always check that the video call service you need is on the allow lists, that it works in your network, and so on. If not, use another one.

```yaml
mode: srv
auth:
  provider: jitsi
room:
  
  id: "https://meet.small-dm.ru/REPLACE_ME_WITH_ROOM_ID" # or https://meet.small-dm.ru/ROOM  or  https://meet1.arbitr.ru/ROOM  or  https://meet.handyweb.org/ROOM etc.

crypto:
  key: "REPLACE_ME_WITH_64_HEX_CHARS"
net:
  transport: datachannel
  dns: "8.8.8.8:53"
data: data
```

Run it:

```sh
./build/olcrtc-linux-amd64 server.yaml
```

## Run the client

Create `client.yaml` on the client machine. `auth.provider`, `room.id`, `crypto.key` and `net.transport` must match the server.

> **Important:** always check that the video call service you need is on the allow lists, that it works in your network, and so on. If not, use another one.

```yaml
mode: cnc
auth:
  provider: jitsi
room:
  id: "https://meet.small-dm.ru/REPLACE_ME_WITH_ROOM_ID" # or https://meet.small-dm.ru/ROOM  or  https://meet1.arbitr.ru/ROOM  or  https://meet.handyweb.org/ROOM etc.
crypto:
  key: "REPLACE_ME_WITH_64_HEX_CHARS"
net:
  transport: datachannel
  dns: "8.8.8.8:53"
socks:
  host: "127.0.0.1"
  port: 8808
data: data
```

Run it:

```sh
./build/olcrtc-linux-amd64 client.yaml
```

After startup SOCKS5 listens on `127.0.0.1:8808`.

## Verify

```sh
curl --socks5-hostname 127.0.0.1:8808 https://icanhazip.com
```

It should return the server IP.

## Control

Stop a manual run with `Ctrl+C`.

If the process runs in the background:

```sh
pgrep -af olcrtc
kill <pid>
```

Update:

```sh
git pull --recurse-submodules
mage build
```

Restart the server and the client with the same YAML configs.

## Multiple instances

You can run several servers or clients on one machine: create a separate YAML for each instance. For clients use different SOCKS5 ports:

```yaml
socks:
  host: "127.0.0.1"
  port: 8809
```

All settings and the compatibility matrix: [settings.md](settings.md). Detailed manual build: [manual.md](manual.md).
