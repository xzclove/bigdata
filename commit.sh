#!/bin/bash
if  [ $1 ==null  ]; then
  echo "please input  update name!"
else
   echo update name is  $1
   git commit -a -m  "$1"
   git push 
fi 
