#!/bin/bash
if  [ $1 ==null  ]; then
  echo "please input  update name!"
else
   git add *
   echo $1
   git commit -m "$1"
   git push 
   git log --color --graph --pretty=format:'%Cred%h%Creset -%C(yellow)%d%Creset %s %Cgreen(%cr) %C(bold blue)<%an>%Creset' --abbrev-commit
fi 
