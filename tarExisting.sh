#!/bin/sh


function getName(){
   local raw=$1
   basename '$raw' | sed -e's/%\([0-9A-F][0-9A-F]\)/\\\\\x\1/g' | xargs echo -e
}

function append(){
  local raw=$1
  filename=$(getName $raw)
  cat $raw | tar -r -f $archive
}
find -type f -print0 | xargs -0 -I'{}' getName '{}'
