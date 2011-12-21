#include <stdlib.h>
#include "parser.h"

int main(int argc, char *argv[])
{
    Parser *parser;

    if (argc != 7)
    {
        printf("Usage: %s <master-id> <worker-id> <num-reducers> <input-file> "
               "<output-path> <kb-mem-limit>\n", argv[0]);
        return -1;
    }

    guint master_id;
    guint worker_id;
    guint num_reducers;
    glong kb_limit;

    master_id = (guint)atoi(argv[1]);
    worker_id = (guint)atoi(argv[2]);
    num_reducers = (guint)atoi(argv[3]);
    kb_limit = (glong)atoi(argv[6]);

    parser = parser_new(master_id, worker_id, num_reducers, argv[4], argv[5]);
    parser_run(parser, kb_limit);
    parser_free(parser);

    return 0;
}
