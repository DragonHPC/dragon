if [[ -z "$PYTHONPATH" ]]; then
  export PYTHONPATH=$PWD
else
  export PYTHONPATH=$PWD:$PWD/integration:$PYTHONPATH
fi

# part of a hack to skip the monkeypatching
# in ./src/dragon/__init.py__
export NODRAGON=1

# to enable access to support scripts needed
# for the shepherd unit tests
export PATH=$PATH:../shepherd

function clean() {
  rm -f ./*.log dump_file
  rm -f /dev/shm/*
  rm -f .dragon_breakpoints
}
