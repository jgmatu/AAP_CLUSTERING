#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mpi.h"
#include "const.h"

/**
 * Use this variable to set the communccation time between nodes and calc
 * the block time of nodes reduce user MPI_Wtime and see manual to see
 * how to use the framework function.
 */
double time_comm;

/**
 * La lógica de los procesos reduce es muy sencilla cuentan el número de ocurrencias
 * que obtienen del proceso map asociadas a su id del primer dígito hasta que reciben
 * una ocurrencia negativa como señal de cierre del proceso master. Terminan la tarea
 * y mandan un mensaje al proceso map con el número de total de ocurrencias de primer
 * dígito asociado a su id.
 */
void
reduce(int id)
{
      MPI_Status stat;
      int total = 0, occurences;

      for(;;) {
            MPI_Recv(&occurences, 1, MPI_INT, MPI_ANY_SOURCE, TAG_REDUCE, MPI_COMM_WORLD, &stat);
            if (occurences < 0) {
                  break;
            }
            total += occurences;
      }
      MPI_Send(&total, 1, MPI_INT, MASTER, TAG_REDUCE, MPI_COMM_WORLD);
}

int
main(int argc, char *argv[]) {
      int id;
      int numprocs;

      MPI_Init(&argc, &argv);
      MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
      MPI_Comm_rank(MPI_COMM_WORLD, &id);

      if (numprocs < MIN_PROCESS) {
            fprintf(stderr, "%s\n", "Not enougth process");
            MPI_Finalize();
            exit(1);
      }

      MPI_Barrier(MPI_COMM_WORLD); // Wait all process ready...
      if (id > MASTER && id <= NUM_REDUCE) {
            reduce(id);
      }
      MPI_Finalize();
      exit(0);
}
