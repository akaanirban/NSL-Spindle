#!/bin/bash
qemu-system-arm -no-reboot \
  -kernel ./qemu-rpi-kernel/kernel-qemu-4.4.13-jessie \
  -cpu arm1176 \
  -m 256 -M versatilepb \
  -serial stdio -append "root=/dev/sda2 panic=1 rootfstype=ext4 rw" \
  -k en-us \
  -hda 2017-04-10-raspbian-jessie-lite.img \
  -redir tcp:2222::22
