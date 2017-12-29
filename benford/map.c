#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include "mpi.h"
#include "const.h"


/**
 * Use this variable to check the comunication time between nodes this
 * variable count the total time of comm between nodes in a single node map.
 * use MPI_Wtime to it... see manual
 */
double time_comm;

/**
 * Función de manejo de errores genéricos del sistema y terminación del
 * sistema con error.
 */
void
exiterr()
{
      fprintf(stderr, "%s\n", strerror(errno));
      MPI_Finalize();
      exit(1);
}

/**
 * Esta funcion de vuelve el primer dígito del número pasado
 * como parámaetro.
 */
long
get_first_numb(long numb)
{
      long result = 0;

      while (numb) {
          result = numb % 10;
          numb /= 10;
      }
      return result;
}

/**
 * Reserva memoria para el buffer de línea y devuelve un error si no ha sido
 * posible reservar la memoria del buffer.
 */
char*
xmalloc_line()
{
      char *msg = (char *)malloc(MAXLINE * sizeof(char));

      if(!msg) {
            exiterr();
      }
      return msg;
}

/**
 * Envia los contadores de primer dígito a los nodos reduce. Cada reduce
 * recibirá un mensaje con el número de ocurrencias de primer dígito del
 * lote leido por el proceso map.
 */
void
send_reduce_numbs(int numbs[])
{
      for (int i = 1; i < NUMBERS; i++) {
            MPI_Send(&numbs[i], 1, MPI_INT, i, TAG_REDUCE, MPI_COMM_WORLD);
      }
}

/**
 * Inicializa los contadores de primer dígito de numero que obtendrá
 * de los lotes de fichero que vaya leyendo.
 */
void
init_numbs(int numbs[])
{
      for (int i = 1; i < NUMBERS; i++) {
            numbs[i] = 0;
      }
}

/**
 * Este procedimiento obtiene el punto inicial de lectura del sub conjunto de datos
 * y el tamaño de lote en bytes a leer del fichero.
 *
 * Leerá linea a linea hasta que cumpla con su tamaño de lote. Cuando termine de leer
 * el lote mandará un mensaje a cada proceso reduce con la cuenta total de números
 * con primer dígito N. Termina cerrando el fichero y liberando el buffer de lectura
 * de lineas del fichero.
 */
void
read_data(char* path, int offset, int batch) {
      FILE* file = fopen(path, "r+");
      if (!file) {
            exiterr();
      }
      if (fseek(file, offset, SEEK_SET) < 0) {
            exiterr();
      }
      size_t bufsize = MAXLINE;
      size_t characters;
      char* buffer = xmalloc_line();
      int nbytes = 0;
      int numbs[NUMBERS];
      init_numbs(numbs);

      while ((characters = getline(&buffer, &bufsize, file)) != EOF && nbytes < batch) {
            nbytes += strlen(buffer);
            numbs[get_first_numb(atol(buffer))]++;
      }
      send_reduce_numbs(numbs);
      free(buffer);
      if (fclose(file) < 0) {
            exiterr();
      }
}

/**
 * Es necesario reinicializar el master cada vez que obtenemos un mensaje nuevo de fichero
 * del proceso master si no quedará constancia del anterior mensaje de fichero y puede
 * no llegar a sobre escribirse dando errores de ejecución en el recibo de la señal de
 * fin...
 */
void
initalize_buffer (char buff[])
{
      for (int i = 0; i < SIZE_BUFF; i++) {
            buff[i] = '\0';
      }
}

/**
 * La lógica del proceso map se basa en escuchar mensajes del proceso master.
 * El proceso máster mandará un mensaje con nuevo trabajo para el proceso map.
 *
 * Cuando se recibe nuevo trabajo se realiza la obtención del inicio de lote, tamaño
 * de lote y fichero de conjunto de datos, estos datos se mandan al procedimiento
 * read_data que relizará la cuenta de números.
 *
 * Cuando el proceso map recibe la señal de fin terminará su tarea y mandará el
 * mensaje de confirmación de cierre al proceso master.
 */
void
map(int id)
{
      char msg[SIZE_BUFF];
      int params[2];
      MPI_Status stat;

      for(;;) {
            initalize_buffer(msg);
            MPI_Recv(&msg, MAXLINE, MPI_CHAR, MASTER, TAG_MAP, MPI_COMM_WORLD, &stat);
            if (strcmp(msg, FIN) == 0) {
                  break;
            }
            MPI_Recv(&params, 2, MPI_INT, MASTER, TAG_MAP, MPI_COMM_WORLD, &stat);
            int offset = params[0];
            int batch = params[1];
            read_data(msg, offset, batch);
      }
      MPI_Send(&id, 1, MPI_INT, MASTER, TAG_MAP, MPI_COMM_WORLD);
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
      if (id > NUM_REDUCE) {
            map(id);
      }
      MPI_Finalize();
      exit(0);
}
