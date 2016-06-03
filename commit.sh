#!/bin/bash
if  [ $1 ==null  ]; then
  echo "please input  update name!"
else
   git add *
   echo $1
   git commit -m "$1"
   git push 
fi 
