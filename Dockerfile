FROM --platform=linux/amd64 python:3.12-slim-bookworm@sha256:8a7e7cc04fd3e2bd787f7f24e22d5d119aa590d429b50c95dfe12b3abe52f48b

RUN printf '%s\n' \
      'deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/20260728T000000Z/ bookworm main' \
      'deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/20260728T000000Z/ bookworm-updates main' \
      > /etc/apt/sources.list \
    && rm -f /etc/apt/sources.list.d/debian.sources \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
      ca-certificates=20230311+deb12u1 \
      git=1:2.39.5-0+deb12u3 \
      openssh-client=1:9.2p1-2+deb12u10 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Dockerfile pit_ledger.py /app/
ENTRYPOINT ["python","/app/pit_ledger.py","cloud-run"]
