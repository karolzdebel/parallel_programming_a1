PILOTHOME = /work/wgardner/pilot
MPEHOME = /work/wgardner/mpe

PILOTHOME = /work/wgardner/pilot
MPEHOME = /work/wgardner/mpe

CC = mpicc
CPPFLAGS += -I$(PILOTHOME)/include -I$(MPEHOME)/include
CFLAGS = -g -O0
LDFLAGS += -L$(PILOTHOME)/lib -lpilot -L$(MPEHOME)/lib -lmpe

bang: bang.c
	$(CC) $<  $(LDFLAGS) -o bang 

clean:
	rm -rf bang
