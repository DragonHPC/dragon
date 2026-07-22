define getinfo
    attach $arg0
    where
    thread apply all bt
end