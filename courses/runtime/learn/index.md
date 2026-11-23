---
title: Build your own container runtime — primer
concepts: [container runtime, OCI image, overlayfs, namespaces, pivot_root]
---

## What a container runtime actually does

A container is not a thing the kernel knows about. It is an ordinary process
that was started in a particular way: with a root filesystem built from an
image, and inside namespaces that give it its own view of mounts and
processes. A container runtime is the program that does that starting.

Under a kubelet sit two layers. containerd (or CRI-O) pulls images, keeps them
on disk and answers the kubelet's CRI calls; runc, beneath it, does the Linux
work of turning a directory and a JSON spec into a running process. This
course builds the parts both of them share, in one program: fetch an image
from a registry, turn its layers into a root filesystem, and run the image's
process in namespaces of its own.

## Where your program runs

Namespaces, mounts and overlayfs are Linux, so your program is built with
`GOOS=linux` for the architecture of the kind nodes and copied into the first
worker, `byok8s-worker`. It runs there as root, inside a systemd scope, so
everything it starts can be stopped with it. Set `GOOS=linux` in your editor,
or `syscall.Mount` and friends will not resolve.

The tester serves a registry of its own on your host and passes its address
in each image reference: `HOST:PORT/public/demo:v1`. It speaks plain HTTP and
the same token protocol Docker Hub does. Every image it serves runs the same
small program, a probe, which prints what it can see from inside — its
arguments, environment, working directory, pid, mounts. That is how a stage
judges the container your runtime built rather than the code that built it.

## The shape of the course

One program, grown command by command:

```
app auth REF                      a pull token
app manifest REF                  what the tag resolves to
app pull [--root DIR] REF         blobs into a content-addressed store
app unpack [--root DIR] REF DEST  layers applied into one directory
app mount [--root DIR] REF ID     an overlay rootfs per container
app umount [--root DIR] ID
app config [--root DIR] REF [ARG…] the process the image means
app run [--root DIR] REF [ARG…]   and running it
```

`--root` is where your runtime keeps everything — blobs, extracted layers,
containers. Each stage passes its own, so stages never see each other's state.

## Two things worth knowing before you start

Everything in an image is addressed by the hash of its content. A tag is the
only mutable name; from the manifest down, every reference is a digest, and a
runtime that does not check what it downloaded against that digest is trusting
the network with root on the node.

A container is a set of choices, not a feature. Each stage from `spawn` on adds
one isolation and you can watch what it changes: a chroot hides files but not
mounts, a mount namespace hides mounts but not the way out, `pivot_root` closes
the way out, and a pid namespace hides every other process. Leave one out and
the container still runs — which is exactly why each one is graded.
