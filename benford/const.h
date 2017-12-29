/**
 * Constates compartidas por los nodos, todos los nodos tienen conocimiento
 * de estas constantes donde e define el numero de procesos mínimo, las etiquetas
 *
 */
enum {
      MASTER = 0,
      TAG_MAP = 132,
      TAG_REDUCE = 133,
      NUM_REDUCE = 9,
      MAXLINE = 100,
      SIZE_BUFF = 64 * 1024,
      MIN_PROCESS = 1 + NUM_REDUCE + 1, // The reduce the master and one map...
      NUMBERS = 10,
      MAXNAME = 255
};
const char* FIN = "fin";
