#include <stdlib.h>
#include "parser.h"

int main(int argc, char *argv[])
{
    Parser *parser;

    if (argc != 4)
    {
        printf("Usage: %s <numreducers> <file> <limit>\n", argv[0]);
        return -1;
    }

    parser = parser_new(atoi(argv[1]), argv[2]);
    parser_run(parser, atol(argv[3]));
    parser_free(parser);

    return 0;
}
