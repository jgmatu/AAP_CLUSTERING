#!/bin/sh

if test $# -ne 1
then
	echo ERROR: Numero de parametros insuficiente. $0 numero de procesos map
	exit 1
fi

mpirun -mca btl ^openib -np 1 ./master : -np 9 ./reduce : -np $1 ./map
