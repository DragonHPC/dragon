To build Dragon with GPU support you need to make sure that dragon-config -a has been called with the correct include and library for cuda.
That only needs to be done once. Everytime the cuda module used needs to be reloaded. The build script builds
the test. The test checks the error code following each operation and will hang or core dump if not working correctly.
An observant programmer may note that the number of tests changes between tests currently. This is due to the parent process sitting in a while loop and doing copy's until the value is not Null. Each one of those loops increments the counter. I'll probably change this in the future.
