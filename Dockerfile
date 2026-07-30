# syntax=docker/dockerfile:1
# Copyright 2026 Naadir Jeewa
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0


ARG GO_VERSION=1.26
ARG XX_VERSION=1.6.1
# tlshd (oracle/ktls-utils) provides the userspace TLS handshake agent that
# services the kernel net/handshake upcall for NVMe-TCP (external PSK, TP8011)
# and NFS (x509, RFC 9289). Ubuntu 24.04 noble universe only ships 0.9, which
# predates NVMe-TCP PSK handshake support, so build a modern release from source.
# Pin the latest stable tag; config lives at /etc/tlshd/config since 1.3.0.
ARG KTLS_UTILS_VERSION=ktls-utils-1.3.1

FROM --platform=$BUILDPLATFORM tonistiigi/xx:${XX_VERSION} AS xx

# tlshd build stage: ubuntu:24.04 so the produced binary links the SAME
# GnuTLS/libkeyutils/libnl sonames as the runtime stage (ABI match).
FROM --platform=$BUILDPLATFORM ubuntu:24.04 AS tlshd-build
COPY --from=xx / /
ARG TARGETPLATFORM
ARG TARGETARCH
ARG KTLS_UTILS_VERSION
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates git autoconf automake libtool make pkg-config \
    && rm -rf /var/lib/apt/lists/*
RUN xx-apt-get update && xx-apt-get install -y --no-install-recommends \
    libc6-dev libgnutls28-dev libkeyutils-dev libnl-3-dev libnl-genl-3-dev libglib2.0-dev gcc
RUN git clone --depth 1 --branch "${KTLS_UTILS_VERSION}" \
    https://github.com/oracle/ktls-utils /src/ktls-utils
WORKDIR /src/ktls-utils
# autogen → configure → make. Install into a staging prefix we can COPY from.
RUN ./autogen.sh \
    && case "$TARGETARCH" in amd64) triplet=x86_64-linux-gnu ;; arm64) triplet=aarch64-linux-gnu ;; esac \
    && export CC="/usr/bin/$triplet-gcc" \
       PKG_CONFIG_LIBDIR="/usr/lib/$triplet/pkgconfig:/usr/lib/pkgconfig:/usr/share/pkgconfig" \
    && ./configure \
        --host="$triplet" \
        --prefix=/usr --sysconfdir=/etc \
    || { cat config.log; exit 1; }
RUN make -j"$(nproc)" \
    && make DESTDIR=/dest install \
    && xx-verify /dest/usr/sbin/tlshd \
    # The stock install ships /etc/tlshd/config (or the older /etc/tlshd.conf);
    # ensure the directory exists in the staged tree regardless of layout.
    && mkdir -p /dest/etc/tlshd

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-bookworm AS build
COPY --from=xx / /
ARG TARGETPLATFORM
ARG TARGETARCH
ARG TARGETOS
WORKDIR /src

# libzfslinux-dev lives in the Debian "contrib" component. Enable it before
# installing the OpenZFS development headers/libraries the cgo build links against.
RUN sed -i 's/Components: main$/Components: main contrib/' /etc/apt/sources.list.d/debian.sources \
    && apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates clang lld pkg-config musl-dev gcc \
    && rm -rf /var/lib/apt/lists/*
RUN xx-apt-get update && xx-apt-get install -y --no-install-recommends \
    libc6-dev libzfslinux-dev gcc

COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CC=xx-clang \
    PKG_CONFIG_SYSROOT_DIR="$(xx-info sysroot)" \
    PKG_CONFIG_LIBDIR="$(xx-info sysroot)/usr/lib/$(xx-clang --print-target-triple)/pkgconfig:$(xx-info sysroot)/usr/lib/pkgconfig:$(xx-info sysroot)/usr/share/pkgconfig" \
    CGO_ENABLED=1 GOOS="$TARGETOS" GOARCH="$TARGETARCH" \
    go build -tags=libzfs -trimpath -ldflags='-s -w' \
    -o /out/zfs-csi ./cmd/zfs-csi \
    && xx-verify /out/zfs-csi

# Runtime base MUST NOT be distroless: the driver dynamically links libzfs
# (libzfs.so.4 / libzfs_core.so.3 / libnvpair.so.3) and mount-utils shells out
# to mount/mkfs.ext4/blkid/mount.nfs4 inside the container. Ship an Ubuntu 24.04
# base carrying the OpenZFS userland (zpool + libzfs runtime) and the mount
# tooling. The driver runs privileged on the host, so it runs as root.
#
# Ubuntu 24.04 (NOT debian:12-slim) is required: Debian 12 ships OpenZFS 2.1
# (libzfs.so.4 = 2.1.x), whose libzfs_init() cannot handshake with the modern
# OpenZFS 2.2/2.3 kernel module the target nodes run (both the CAPA Ubuntu 24.04
# AMI and the KubeVirt golden Ubuntu 24.04 image) — provisioning fails with
# "libzfs create: Failed to initialize the libzfs library". Ubuntu 24.04's
# zfsutils-linux is OpenZFS 2.2, which matches. The libzfs.so.4 soname is stable
# 2.1->2.2 so the binary built against Debian's 2.1 headers loads the 2.2 runtime
# library by soname without an ABI change.
FROM ubuntu:24.04
# nfs-kernel-server remains required for the kernel nfsd support and procfs
# interface. The storage agent exclusively acquires, starts, and stops the
# host-global kernel nfsd; host nfs-server/mountd ownership must be disabled or
# lifecycle startup fails closed on collision. The agent bypasses sharenfs/libshare
# and self-mounts the nfsd pseudo-filesystem while binding /var/lib/nfs.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    zfsutils-linux \
    kmod \
    util-linux \
    e2fsprogs \
    xfsprogs \
    nfs-common \
    nfs-kernel-server \
    # tlshd runtime shared libraries: GnuTLS (TLS engine), keyutils (reads PSK /
    # cert serials from the kernel keyring the handshake upcall passes), and
    # libnl (generic-netlink transport for the net/handshake upcall itself).
    libgnutls30 \
    libkeyutils1 \
    libnl-3-200 \
    libnl-genl-3-200 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/* \
    # OpenZFS libshare's NFS backend writes exports to /etc/exports.d/zfs.exports.
    # If this directory is absent (it is not created by the packages above), the
    # backend is disabled at init and the cgo zfs_share() becomes a silent no-op
    # (returns 0 but exports nothing). The zfs CLI creates it on demand; the
    # in-process libzfs binding does not, so pre-create it here.
    && mkdir -p /etc/exports.d /etc/tlshd
COPY --from=build /out/zfs-csi /zfs-csi
# tlshd: the userspace TLS handshake agent for kernel NVMe-TCP + NFS transports.
# The chart runs it as a privileged, hostNetwork sidecar in the node + storage
# DaemonSets so its net/handshake upcall handler shares init_net with the
# kernel-created transport sockets. Shipping it in the same image keeps one
# artifact; the sidecar overrides the entrypoint to `tlshd`.
COPY --from=tlshd-build /dest/usr/sbin/tlshd /usr/sbin/tlshd
COPY --from=tlshd-build /dest/etc/tlshd /etc/tlshd
ENTRYPOINT ["/zfs-csi"]
