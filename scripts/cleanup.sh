#!/bin/sh
EXTS=('.pyc' '~' '.o')

for i in ${EXTS[@]}; do
  find . -name "*$i" -exec rm {} \;
done
