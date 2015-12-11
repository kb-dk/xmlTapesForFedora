#!/bin/bash
#This scripts creates tar files out of the backend files of a normal fedora.
#Param 1 is the name of the tape file
#Param 2 is the folder, whose contents to tar.

#The tricky part of the script is to transform the filenames to the format used by the tape archive

set -e


archive="$1"
scanfolder="$2"



function getName(){
   local raw="$@"
   #URL Decode
   basename "$raw" | sed -e's/%\([0-9A-F][0-9A-F]\)/\\\\\x\1/g' | xargs echo -n -e
   echo -n '#'
   #Add the timestamp to the filename
   date '+%s%3N'
}

function append(){
  local raw="$@"
  filename=$(getName "$raw")
  echo "adding $filename"
  temp=$(mktemp)
  gzip -c "$raw" > "$temp"
  tar -r -P -f "$archive" "$temp" --transform="s|.*|$filename.gz|"
  rm "$temp"
}



echo "Starting and scanning folder $scanfolder"
for file in $( find "$scanfolder" -type f ); do
    append "$file"
done

