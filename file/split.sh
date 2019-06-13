#!/bin/bash
cd "$1" || exit
split -l "$2" "$3" "$3"_
