cc=gcc
cflags=-fPIC -I/usr/include/python2.7
ldflags=-shared -lpython2.7
sources=ubkey.c
target=ubkey.so

all: $(target)
$(target) : $(objects)
	$(cc) $(cflags) $(ldflags) -o $@ $(sources)
