--[[

    file dragon module

    Copyright 2019-2022 Hewlett Packard Enterprise Development LP

]]--

-- reasons to keep module from continuing --


-- standard Lmod functions --


help ([[

This modulefile sets up your environment to
use the Dragon distributed runtime for
Python multiprocessing.

]])

whatis("dragon - Dragon distributed runtime and Python multiprocessing")


-- module variables --


local base_dir = "dirname $(dirname " .. myFileName() .. ")"
local DRAGON_BASE_DIR = capture(base_dir):gsub("\n$", "")


-- environment modifications --

setenv("DRAGON_VERSION", "0.10")

setenv("DRAGON_BASE_DIR", DRAGON_BASE_DIR)

prepend_path("PATH", pathJoin(DRAGON_BASE_DIR, "bin"))
prepend_path("LD_LIBRARY_PATH", pathJoin(DRAGON_BASE_DIR, "lib"))

setenv("DRAGON_DEBUG", "")
