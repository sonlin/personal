#!/bin/bash

str="hello,world,i,like,you,babalala"
arr=(${str//,/ })

for i in ${arr[@]}
do
	echo $i
done
