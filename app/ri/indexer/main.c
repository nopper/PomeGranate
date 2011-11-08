#include <stdio.h>
#include <stdlib.h>
#include "reducer.h"

struct Person {
    int age;
    int height;
};

int main(int argc, char *argv[])
{
    printf("int: %d guint: %d\n", sizeof(unsigned int), sizeof(guint));

    if (argc < 2)
    {
        printf("Usage: %s <outputfile> <int>..\n", argv[0]);
        return -1;
    }

    int *ids = (int *)malloc(sizeof(int) * (argc - 2));

    for (int i = 2; i < argc; i++)
        *(ids + (i - 2)) = atoi(argv[i]);

    reduce(argc - 2, ids);

    free(ids);

#if 0
    Posting post;
    FileReader *reducer = file_reader_new(0, 592345);
    int i = 0;

    while (!file_reader_next(reducer, &post))
    {
        printf("%d -> %d\n", post.docid, post.occurrence);
    }

    printf("Read %d\n", i);


    return 0;

    struct Person *users = malloc(sizeof(struct Person) * 20);

    for (int i = 0; i < 20; i++)
    {
        users[i].age = i;
        users[i].height = 2 * i;
        printf("Person[%d] = %p age: %d height: %d\n", i, users[i], users[i].age, users[i].height);
    }

    printf("\n");

    int nfile = 20, i = 4;
    int stop = nfile - i - 1;

    for (int j = 0; j < stop; j++, i++) {
        users[i] = users[i + 1];
    }
    nfile--;

    for (int i = 0; i < nfile; i++)
    {
        printf("Person[%d] = %p age: %d height: %d\n", i, &users[i], users[i].age, users[i].height);
    }
#endif
}
