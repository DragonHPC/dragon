import argparse
import mpi4py
mpi4py.rc.initialize = False
import numpy as np
from time import perf_counter as timer

from dragon import DDict

plotOn = False  # set to True to generate a plot per rank

if plotOn:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

from mpi4py import MPI

def get_args():
    parser = argparse.ArgumentParser(description="1d Van Leer Advection Solver")
    parser.add_argument(
        "--cfl",
        type=float,
        default=0.8,
        help="CFL (aka Courant number) to use in the solver",
    )
    parser.add_argument(
        "--dser",
        type=str,
        default="",
        help="Serialize DDict descriptor to write data to",
    )
    return parser.parse_args()

# van Leer limiter function
def van_leer(r):
    if r <= 0.0:
        return 0.0
    return (r + np.abs(r)) / (1.0 + r)

def apply_bounds(u, comm):
    rank = comm.Get_rank()
    size = comm.Get_size()

    rank_l = rank - 1
    if rank_l < 0:
        rank_l = size - 1
    rank_r = rank + 1
    if rank_r >= size:
        rank_r = 0

    # Apply periodic boundary conditions for the slopes
    comm.Sendrecv([u[2:4], 2, MPI.DOUBLE], dest=rank_l, sendtag=0, recvbuf=[u[-2:], 2, MPI.DOUBLE], source=rank_r, recvtag=0)
    comm.Sendrecv([u[-4:-2], 2, MPI.DOUBLE], dest=rank_r, sendtag=1, recvbuf=[u[:2], 2, MPI.DOUBLE], source=rank_l, recvtag=1)

def time_step(u, dt, dx, c):
    lu = len(u)
    F = np.zeros_like(u)

    if c > 0:
        for i in range(2, lu-1):
            deltap = u[i] - u[i-1]
            deltam = u[i-1] - u[i-2]
            if deltam == 0.0:
                r = 0.0
            else:
                r = deltap / deltam
            uL = u[i-1] + 0.5 * van_leer(r) * deltam
            F[i-1] = c * uL

    else:
        for i in range(2, lu-1):
            deltap = u[i+1] - u[i]
            deltam = u[i] - u[i-1]
            if deltap == 0.0:
                r = 0.0
            else:
                r = deltam / deltap
            uR = u[i] - 0.5 * van_leer(r) * deltap
            F[i-1] = c * uR

    u[2:-2] = u[2:-2] - (dt / dx) * (F[2:-2] - F[1:-3])

def main(cfl, ddict):

    MPI.Init()  # now we can initializatize MPI

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    print(f"Rank {rank} of {size} says: Hello from MPI!", flush=True)
    print(f"Using CFL={cfl} and provided DDict={ddict}", flush=True)

    # Parameters
    nx = 256                    # Number of grid cells per rank
    Lx = 1.0                    # Domain size
    dx = Lx / (size * nx)       # Cell size
    c = 1.0                     # Advection speed
    t_final = 0.1               # Final time
    dt = cfl * dx / np.abs(c)   # Time step

    # Grid setup (cell centers)
    mystart = (rank * nx * dx + dx / 2) + dx / 2
    myend = mystart + nx * dx
    x = np.linspace(mystart, myend, nx)

    # Initial condition (a square wave)
    u = np.where((x > 0.1) & (x < 0.3), 1.0, 0.0)

    # Add in boundary zones
    u = np.pad(u, 2, mode='wrap')

    # Time stepping loop
    time = 0.0
    step = 0
    tot_elap = timer()
    while time < t_final:
        t0 = timer()
        apply_bounds(u, comm)
        time_step(u, dt, dx, c)
        time += dt
        step += 1
        ttot = timer() - t0
        cps = size * nx / ttot
        if rank==0:
            print(f"Step = {step:6d} : SimTime = {time:.4e} : StepElapsed = {ttot:.2e} s : CellsPS = {cps:.2e}", flush=True)
        if plotOn and step%100 == 0:
            plt.plot(u[2:-2])
            plt.savefig(f"my_plot_{rank}.png")

    # write the results to the DDict if we have one
    if ddict is not None:
        ddict[f"cfl_{cfl}_rank{rank}"] = u[2:-2]

    # Write something unique from this MPI rank into the provided shared output Queue
    comm.Barrier()
    tot_elap = timer() - tot_elap
    if rank==0:
        print(f"Simulation complete...", flush=True)
        print(f"Total execution time = {tot_elap:.2e} seconds", flush=True)
    MPI.Finalize()

if __name__ == '__main__':
    args = get_args()
    if args.dser != "":
        ddict = DDict.attach(args.dser)
    else:
        ddict = None

    main(args.cfl, ddict)