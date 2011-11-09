CC=gcc --std=c99 -g -O0 -fno-omit-frame-pointer
#CC=gcc --std=c99 -O3 -fomit-frame-pointer
CFLAGS=`pkg-config --cflags glib-2.0`
LDFLAGS=`pkg-config --libs glib-2.0`

reducer: main.o reducer.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o reducer reducer.c main.c

clean:
	rm reducer

all: reducer
