
--[[

    file dragon-dev module

    Copyright 2019-2022 Hewlett Packard Enterprise Development LP

]]--


-- reasons to keep module from continuing --
--NOTE: section to setup pre-reqs families and/or conclifts


-- standard Lmod functions --
-- NOTE: good to keep these at the top of a modulefile. That way these basic functions
-- can still be run if there are issues in the "environment modifications" section


help ([[

This modulefile sets up your environment to
build and use Dragon for development.

]])

whatis("dragon-dev - Dragon development environment")


-- template variables --
-- NOTE: and variables that rely on a Makefile template or used throughout module

local base_dir = "dirname $(dirname " .. myFileName() .. ")"
local DRAGON_BASE_DIR = capture(base_dir):gsub("\n$", "")


-- environment modifications --

setenv("DRAGON_VERSION", "0.7")

-- get project dir
setenv("DRAGON_BASE_DIR", DRAGON_BASE_DIR)

-- NOTE: one can site prereqs or auto-load what is wanted,
-- lmod will take care of the rest

-- module load: "load" will fail if module cannot be loaded
-- module load: "try_load" will still load a module if the module couldn't be loaded
-- module load: "depends_on" Loads all modules. When unloading only dependent modules are unloaded.

load("craype-x86-rome")
load("PrgEnv-gnu");
load("cray-python")


-- Kaylie -> would like to know more about this version number ....
-- Could simplify this code for future
local gccv = tonumber(subprocess("gcc -dumpversion | cut -f1 -d."))
local nogppf = tonumber(subprocess("g++ --version > /dev/null 2>&1 || echo 0"))
if (gccv < 9) then
    LmodWarning("gcc version is too old to build Dragon")
end

if (nogppf == 0) then
    LmodWarning("g++ is not installed and Dragon will not build")
end

-- aliases for dragon entry points (dev env conveniences)
set_alias("dragon", "python3 -m dragon.cli dragon")
set_alias("dragon-backend", "python3 -m dragon.cli dragon-backend")
set_alias("dragon-localservices", "python3 -m dragon.cli dragon-localservices")
set_alias("dragon-globalservices", "python3 -m dragon.cli dragon-globalservices")
set_alias("dragon-tcp", "python3 -m dragon.cli dragon-tcp")
set_alias("dragon-hsta-if", "python3 -m dragon.cli dragon-hsta-if")

-- see https://lmod.readthedocs.io/en/latest/050_lua_modulefiles.html#extra-functions
if (isDir(DRAGON_BASE_DIR)) then
    setenv(         "DRAGON_INCLUDE_DIR",    pathJoin(DRAGON_BASE_DIR, "include"))
    setenv(         "DRAGON_LIB_DIR",        pathJoin(DRAGON_BASE_DIR, "lib"))
    prepend_path(   "LD_LIBRARY_PATH",       pathJoin(DRAGON_BASE_DIR, "lib"))
    prepend_path(   "PATH",                  pathJoin(DRAGON_BASE_DIR, "bin"))
    prepend_path(   "PYTHONPATH",            DRAGON_BASE_DIR)
else
    -- NOTE: this should proboly be an LmodError, which will cause the module to not be loaded
    LmodWarning(myFileName() .. " Unable to find required directory.")
end

setenv("DRAGON_BUILD_NTHREADS", "6")
