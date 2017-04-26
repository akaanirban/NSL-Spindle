#!/bin/bash
qemu-system-arm -no-reboot \
  -kernel ./qemu-rpi-kernel/kernel-qemu-3.10.25-wheezy \
  -cpu arm1176 \
  -m 256 -M versatilepb \
  -serial stdio -append "root=/dev/sda2 panic=1 rootfstype=ext4 rw" \
  -clock dynticks \
  -hda 2015-02-16-raspbian-wheezy.img \
  -redir tcp:2222::22
