#!/bin/bash
set -e


archive="$1"
scanfolder="$2"

function getName(){
   local raw=$@
   basename "$raw" | sed -e's/%\([0-9A-F][0-9A-F]\)/\\\\\x\1/g' | xargs echo -n -e
   echo -n '#'
   date '+%s000'
}

function append(){
  local raw=$@
  filename=$(getName "$raw")
  mkdir -p $(dirname "$filename")
  cp "$raw" "$filename"
  tar -r -f "$archive" "$filename"
  rm "$filename"
}



for file in $( find "$scanfolder" -type f ); do
    append "$file"
done

