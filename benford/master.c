#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <math.h>
#include "mpi.h"
#include "const.h"

/**
 * Use this variable to calc the communccation time betwee nodes
 * in the node master and cacl total block time.... Use MPI_Wtime to it...
 * see manual.
 */
double time_comm;

/**
 * This fuction calc the table of Benford law to compare with our data
 * number. We obtain a table of Benford numbers from 1 to 9.
 *
 * This funcion is used with malloc don't forget free this memory returned...
 */
float*
xmalloc_benford_table()
{
      float* table = malloc(sizeof(float) * NUMBERS);

      for (int n = 1; n < NUMBERS; n++) {
            table[n] = log10(n + 1) - log10(n);
      }
      return table;
}

/**
 * Obtener el total de ocurrencias de todos los numeros obtenido de map reduce
 * para una siguiente calculo de frecuencias del resultado de numeros obtenidos.
 */
float
get_total_numbers(int numbs[])
{
      float total = 0;

      for (int n = 1; n < NUMBERS; n++) {
            total += numbs[n];
      }
      return total;
}

/**
 * Este procedimiento comprueba el cumplimiento de la ley de Benford
 * dado el resultado final de Map Reduce el proceso master hace el calculo
 * final de si se cumple o no la ley de Benford. Realiza un calculo de frecuencias
 * de las ocurrencias de cada numero y comprueba con la formula de la ley de Benford
 * si el resultado de comparar la tabla de bendord con la frecuencia del numero Esta
 * por encima de un umbral se descarta que ese conjunto de numeros cumpla con la ley
 * de benford de los numeros naturales...
 */
void
calc_benford_law(int numbs[])
{
      float* table = xmalloc_benford_table();
      float total = get_total_numbers(numbs);
      int success = 1;

      for (int n = 1; n < NUMBERS; n++) {
            if (total == 0 || fabs(numbs[n] / total - table[n]) > 0.03) {
                  success = 0;
            }
      }
      if (success) {
            fprintf(stderr, "%s\n", "Benford law success");
      } else {
            fprintf(stderr, "%s\n", "Benford law fail");
      }
      free(table);
}

/**
 * Este procedimiento muestra los resultados finales de ocurrencia de los números
 * del 1 al 9 por panatalla.
 *
 */
void
print_resuts(int numbs[])
{
      for (int i = 1; i < NUMBERS; i++) {
            fprintf(stdout, "The number %d appear %d times...\n", i, numbs[i]);
      }
}

/**
 * Una vez se manda la señal de cierre a los procesos reduce se espera a que
 * terminen su ejecución y manden los resultados al proceso máster el proceso
 * máster una vez termine de recibir los mensajes de los procesos reduce podrá
 * pintar los resultados finales por la entrada estándar.
 *
 */
void
wait_numbs_reduce(int numbs[])
{
      int reduce_value;
      MPI_Status stat;

      for (int id_reduce = 1; id_reduce <= NUM_REDUCE; id_reduce++) {
            MPI_Recv(&reduce_value, 1, MPI_INT, id_reduce, TAG_REDUCE, MPI_COMM_WORLD, &stat);
            numbs[id_reduce] = reduce_value;
      }
}

/**
 * El envío de cierre a los procesos reduce se realiza mandando un número de
 * ocurrencias que no tenga sentido. En este caso se mandan un número negativo.
 *
 * Cuando un proceso reduce recibe un número negativo sabe que es la señal de cierre
 * del proceso máster, termina su tarea y manda los resultados al proceso master.
 */
void
close_reduces()
{
      int close = -1;

      for (int id_reduce = 1; id_reduce <= NUM_REDUCE; id_reduce++) {
            MPI_Send(&close, 1, MPI_INT, id_reduce, TAG_REDUCE, MPI_COMM_WORLD);
      }
}

/**
 * Este procedimiento espera por la confirmación de cierre de los procesos map
 * no podemos cerrar los reduce hasta que no hayan acabado el trabajo los
 * procesos map por eso es importante el recibo del mensaje de confirmación
 * de cierre de los map.
 */
void
wait_map_close(int numprocs)
{
      int map_id = -1;
      MPI_Status stat;

      for (int i = 1; i <= numprocs - (NUM_REDUCE + 1); i++) {
            MPI_Recv(&map_id, 1, MPI_INT, MPI_ANY_SOURCE, TAG_MAP, MPI_COMM_WORLD, &stat);
      }
}

/**
 * Una vez se termina el envío de trabajo por lotes a los procesos map se envía la
 * señal de cierre "fin" cuando los nodos map reciben este mensaje saben que se
 * ha terminado el envío de trabajo terminan su tarea y envía un mensaje al master
 * de confirmación del cierre.
 */
void
close_maps(int numprocs)
{
      char buff[SIZE_BUFF];

      sprintf(buff, "%s", FIN);
      for (int id_map = NUM_REDUCE + 1; id_map < numprocs; id_map++) {
            MPI_Send(buff, strlen(buff), MPI_CHAR, id_map, TAG_MAP, MPI_COMM_WORLD);
      }
}

/**
 * Este es el procedimiento de cierre del sistema. Primero se manda la señal de
 * cierre a los procesos map. Una vez cerrados los procesos map y recibida la confirmación
 * de cierre se manda la señal de cierre a los procesos reduce con todos los map cerrados.
 *
 * Los procesos reduce irán cerrando y mandarán los resultados al nodo maestro que guardará
 * los resultados y terminará mostrándolos por pantalla.
 */
void
close_system(int numprocs)
{
      close_maps(numprocs);
      wait_map_close(numprocs);
      close_reduces(numprocs);

      int numbs[NUMBERS];
      wait_numbs_reduce(numbs);
      calc_benford_law(numbs);
}

/**
 * Procedimiento genérico de cierre del nodo por
 * error en el sistema.
 */
void
exiterr(int numprocs)
{
      close_system(numprocs);
      fprintf(stderr, "%s\n", strerror(errno));
      MPI_Finalize();
      exit(1);
}

/**
 * Este procedimiento obtiene el nuevo proceso id al que hay que mandar trabajo
 * Es un procedimiento de incremento cíclico, cuando se llega a un límite de
 * id de proceso de map vuelve a empezar por el primer proceso map del sistema.
 */
int
next_id_map(int id_map, int numprocs)
{
      id_map++;
      if (id_map >= numprocs) {
            id_map = NUM_REDUCE + 1;
      }
      return id_map;
}

/**
 * función que coprueba si el fichero es un fichero regular es decir un fichero
 * de texto que pueda ser abierto para lectura de datos.
 */
int
isregfile(char *file)
{
	struct stat sb;

	if (stat(file , &sb) < 0) {
		fprintf(stderr, "Error stat file... %s error... %s\n" , file , strerror(errno));
            exit(1);
	}
	switch (sb.st_mode & S_IFMT) {
	case S_IFREG:
		return 1;
	default:
		return 0;
	}
}

/**
 * Comprueba la extensión del archivo. El sistema solo realizará lectura de
 * conjuntos de datos que tengan extensión .txt en la entrada de directorio de
 * lectura.
 */
int
isext(char *file , char *ext)
{
	return strcmp(file + strlen(file) - strlen(ext) , ext) == 0;
}

/**
 * Función que comprueba el fichero de la entrada de directorio si el fichero
 * se detecta como conjunto de datos se envía al siguiente procedimiento de cálculo
 * de lotes para enviar el trabajo a los procesos map.
 */
int
is_data_file(char *file)
{
      return isext(file , ".txt") && isregfile(file);
}

/**
 * Esta función devuelve el tamaño total de un conjunto de datos por una
 * llamada al sistema sin tener que recorrer todo el conjunto. Esta información
 * es necesaria para calcular el tamaño de lote.
 */
long
get_size_file(char* file)
{
      struct stat sb;

	if (stat(file , &sb) < 0) {
		fprintf(stderr, "Error stat file... %s error... %s\n" , file , strerror(errno));
		exit(1);
	}
      return sb.st_size;
}

/**
 * Función que recibe el número de procesos y el tamaño de fichero. Calcula el número
 * de procesos map del sistea y obtiene el tamaño de lote de trabajo para los mao
 * de manera uniforme.
 */
long
get_batch(int size, int numprocs)
{
      int num_maps = numprocs - (NUM_REDUCE + 1);

      return  size / num_maps;
}

/**
 * Este es el procedimiento de envío de información. Este procedimiento madnará
 * a los nodos map el trabajo que deben realizar.
 * Recoge:
 *    - El tamaño de lote.
 *    - El inicio de lectura del lote.
 *    - El nombre del fichero
 * Este procedimiento manda en dos mensajes uno para los parámtros del lote y otro
 * con el nombre de fichero del conjunto de datos y envía la información al nodo
 * map que corresponda.
 */
void
send_map_info(char* filename, int id_map, int numprocs)
{
      int size = get_size_file(filename);
      int batch = get_batch(size, numprocs);
      int params[2];

      for (int init = 0; init < size; init += batch) {
            params[0] = init;
            params[1] = batch;

            MPI_Send(filename, strlen(filename), MPI_CHAR, id_map, TAG_MAP, MPI_COMM_WORLD);
            MPI_Send(params, 2, MPI_INT, id_map, TAG_MAP, MPI_COMM_WORLD);
      }
}

/**
 * We need clean the memory reserved to file name the next file we need
 * the memory clean to concat the path to the new file name...
 */
void
clear_filename(char* filename)
{
      for (int i = 0; i < MAXNAME; i++) {
            filename[i] = '\0';
      }
}

/**
 * This function set up the path to file that is openend from directory
 * selected. Get the path and the file name and create the route to file
 * to check stat and open the file from map...
 */
void
set_path_file(char* filename, char* path, char* file)
{
      filename = strncpy(filename, path, MAXNAME);
      filename = strncat(filename, file, MAXNAME);
}

/**
 * El proceso maestro realiza el control del programa Map Reduce... Es el desarrollo con la lógica
 * compleja del algoritmo. Manda la información de trabajo de lotes a los maps y cuando acaba
 * realiza el proceso del cierre del sistema.
 * La lectura de trabajo lo realiza a través de la entrada de directorio, lee los ficheros con extensión
 * txt y de tipo regular.
 */
void
master(char* path, int numprocs)
{
      DIR* dir = opendir(path);
      if (!dir) {
            exiterr(numprocs);
      }
      char* filename = malloc(sizeof(char) * MAXNAME);
      if (!filename) {
            exiterr(numprocs);
      }
      int id_map = NUM_REDUCE + 1;
      struct dirent* entry;

      while ((entry = readdir(dir)) != NULL) {
            set_path_file(filename, path, entry->d_name);
            if (is_data_file(filename)) {
                  send_map_info(filename, id_map, numprocs);
                  id_map = next_id_map(id_map, numprocs);
            }
            clear_filename(filename);
      }
      free(filename);
      if (closedir(dir) < 0) {
            exiterr(numprocs);
      }
      close_system(numprocs);
}

int
main(int argc, char *argv[])
{
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
      if (id == MASTER) {
            master("benford/", numprocs);
      }
      MPI_Finalize();
      exit(0);
}
