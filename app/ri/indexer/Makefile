RM=rm
CC=gcc -g -O0 -fno-omit-frame-pointer
#CC=gcc -march=x86-64 -mtune=generic -O2 -pipe -fstack-protector --param=ssp-buffer-size=4 -D_FORTIFY_SOURCE=2
CFLAGS=`pkg-config --cflags glib-2.0 libarchive` -I/usr/include
LDFLAGS=`pkg-config --libs glib-2.0 libarchive`

all: reducer mapper

mapper:
	cd map; $(CC) $(CFLAGS) $(LDFLAGS) -o mapper mapper.c parser.c libstemmer/*.c ../utils/utils.c -I ../utils

reducer:
	cd reduce; $(CC) $(CFLAGS) $(LDFLAGS) -o reducer reducer.c libreducer.c ../utils/utils.c -I ../utils

clean:
	$(RM) -f reduce/reducer map/mapper
