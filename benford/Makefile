CC = mpicc
CFLAGS = -g -Wall -Wshadow

PROGRAMS = master reduce map

all: $(PROGRAMS)

% : %.c
	$(CC) $(CFLAGS) $< -o $@ -lm

.PHONY : clean

clean:
	rm $(PROGRAMS)
