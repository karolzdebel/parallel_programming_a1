PILOTHOME = /work/wgardner/pilot
MPEHOME = /work/wgardner/mpe

CC = mpicc
CPPFLAGS += -I$(PILOTHOME)/include -I$(MPEHOME)/include
CFLAGS = -g -O0
LDFLAGS += -L$(PILOTHOME)/lib -lpilot -L$(MPEHOME)/lib -lmpe

FC = mpif90
FFLAGS += -I$(PILOTHOME)/include

all: bang

bang: bang.c

clean:
	rm -rf bang
