--[[

    file dragon module

    Copyright 2019-2022 Hewlett Packard Enterprise Development LP

]]--


-- reasons to keep module from continuing --


-- standard Lmod functions --


help ([[

dragon-python module TODO

]])

whatis("dragon-python - TODO")


-- module variables --


local DRAGON_EXTERNAL_DIR = os.getenv("DRAGON_BASE_DIR") or ""
local DRAGON_CPYTHON_PATH = os.getenv("DRAGON_CPYTHON_PATH") or ""


-- environment modifications --


load("dragon-dev")


if (isDir(DRAGON_EXTERNAL_DIR)) then
    setenv("DRAGON_CPYTHON_PATH", pathJoin(DRAGON_EXTERNAL_DIR, "cpython"))
else
    LmodWarning(myFileName() .. " Unable to find required directory.")
end

if (isDir(DRAGON_CPYTHON_PATH)) then
    prepend_path("PATH", pathJoin(DRAGON_CPYTHON_PATH, "bin"))
else
    LmodWarning(myFileName() .. " Unable to find required path.")
end
