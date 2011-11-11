#include <stdlib.h>
#include "parser.h"

int main(int argc, char *argv[])
{
    Parser *parser;

    if (argc != 5)
    {
        printf("Usage: %s <num-reducers> <input-file> "
               "<output-path> <kb-mem-limit>\n", argv[0]);
        return -1;
    }

    parser = parser_new((guint)atoi(argv[1]), argv[2], argv[3]);
    parser_run(parser, (glong)atoi(argv[4]));
    parser_free(parser);

    return 0;
}
