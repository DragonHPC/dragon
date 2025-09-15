#!/bin/bash
#SBATCH --nodes=128
#SBATCH --exclusive

source ~/.bashrc
conda activate _env312

# For small messages:
dragon gups_ddict.py --num_nodes=128 \
                     --nclients=8192 \
                     --managers_per_node=8 \
                     --total_mem_size=24 \
                     --mem_frac=0.5 \
                     --iterations=1
## 0.12
## DDict GUPS Benchmark
##   Running on 128 nodes (nclients=8192)
##   1024 DDict managers
##   128 DDict nodes
##   24.0 GB total DDict memory (12.0 GB for keys+values)
##
##  Value [B]  Iters  keys/client  min(ops/s)  max(ops/s)  sum(ops/s)   sum(GB/s)
##       1024      1           38   8.560E+00   6.841E+01   1.704E+05   1.625E-01
##       2048      1           38   1.139E+01   6.465E+01   1.688E+05   3.219E-01
##       4096      1           38   1.030E+01   1.066E+02   1.418E+05   5.407E-01
##       8192      1           34   9.944E+00   6.998E+01   1.747E+05   1.333E+00
##      16384      2           29   1.037E+01   1.087E+02   1.796E+05   2.741E+00
##      32768      4           22   1.500E+01   1.420E+02   2.984E+05   9.106E+00
##      65536      7           15   1.438E+01   1.777E+02   2.846E+05   1.737E+01
##     131072     11            9   1.143E+01   1.700E+02   2.956E+05   3.608E+01
##     262144     17            5   1.600E+01   2.017E+02   3.511E+05   8.572E+01
##     524288     24            2   1.666E+01   1.904E+02   3.716E+05   1.815E+02
##    1048576     32            1   1.861E+01   9.452E+02   3.932E+05   3.840E+02

## 0.11 release on hotlum:
## DDict GUPS Benchmark
##   Running on 128 nodes (nclients=8192)
##   1024 DDict managers
##   128 DDict nodes
##   24.0 GB total DDict memory (12.0 GB for keys+values)
##
##  Value [B]  Iters  keys/client  min(ops/s)  max(ops/s)  sum(ops/s)   sum(GB/s)
##       1024      1           38   8.002E+00   3.541E+01   1.296E+05   1.236E-01
##       2048      1           38   4.130E+00   5.921E+01   1.317E+05   2.512E-01
##       4096      1           38   5.201E+00   4.790E+01   1.348E+05   5.143E-01
##       8192      1           34   3.728E+00   4.602E+01   9.264E+04   7.068E-01
##      16384      2           29   6.542E+00   5.147E+01   1.748E+05   2.667E+00
##      32768      4           22   1.186E+01   5.775E+01   2.217E+05   6.766E+00
##      65536      7           15   8.966E+00   4.971E+01   1.377E+05   8.402E+00
##     131072     11            9   1.340E+01   5.750E+01   2.325E+05   2.838E+01
##     262144     17            5   1.541E+01   7.051E+01   2.511E+05   6.130E+01
##     524288     24            2   1.151E+01   1.150E+02   2.702E+05   1.319E+02
##    1048576     32            1   1.377E+01   1.793E+02   2.558E+05   2.498E+02

# for large messages:
dragon gups_ddict.py --num_nodes=128 \
                     --nclients=8192 \
                     --managers_per_node=8 \
                     --total_mem_size=684 \
                     --mem_frac=0.5 \
                     --iterations=1 \
                     --value_size_min=1048576 \
                     --value_size_max=67108864

## 0.12
## DDict GUPS Benchmark
##   Running on 128 nodes (nclients=8192)
##   1024 DDict managers
##   128 DDict nodes
##   684.0 GB total DDict memory (342.0 GB for keys+values)
##
##  Value [B]  Iters  keys/client  min(ops/s)  max(ops/s)  sum(ops/s)   sum(GB/s)
##    1048576      1           41   4.511E+00   5.291E+01   7.284E+04   7.113E+01
##    2097152      2           21   8.350E+00   7.091E+01   1.540E+05   3.008E+02
##    4194304      4           10   9.288E+00   4.427E+01   1.321E+05   5.159E+02
##    8388608      7            5   6.193E+00   3.232E+01   7.707E+04   6.021E+02
##   16777216     11            2   3.606E+00   2.339E+01   4.669E+04   7.295E+02
##   33554432     17            1   2.277E+00   3.047E+01   3.209E+04   1.003E+03
## Calculated number of keys per client is zero. No test for 67108864 B to run.

## 0.11 release on hotlum
## DDict GUPS Benchmark
##   Running on 128 nodes (nclients=8192)
##   1024 DDict managers
##   128 DDict nodes
##   684.0 GB total DDict memory (342.0 GB for keys+values)
##
##  Value [B]  Iters  keys/client  min(ops/s)  max(ops/s)  sum(ops/s)   sum(GB/s)
##    1048576      1           41   4.268E+00   1.983E+01   6.249E+04   6.102E+01
##    2097152      2           21   6.759E+00   2.844E+01   1.131E+05   2.210E+02
##    4194304      4           10   7.812E+00   4.706E+01   1.254E+05   4.899E+02
##    8388608      7            5   7.582E+00   2.538E+01   8.092E+04   6.322E+02
##   16777216     11            2   4.624E+00   2.010E+01   5.171E+04   8.080E+02
##   33554432     17            1   3.219E+00   2.537E+01   3.646E+04   1.139E+03
## Calculated number of keys per client is zero. No test for 67108864 B to run.
