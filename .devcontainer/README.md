See https://github.com/microsoft/vscode-dev-containers and
https://github.com/devcontainers/features for more information.


# Speed up command prompt

Using Git in a devcontainer may be problematic on MacOS or Windows since the
runtime (e.g., Docker Desktop, Podman) will actually run containers in a Linux
VM. Refreshing the Git index (e.g., by runnnig `git status`) may alleviate the
issue, at least until it is refreshed on the host.

Since the codespaces theme includes Git status in the shell prompt by default,
it may seem like a devcontainer is slow or unresponsive with each prompt. The
most effective way to prevent this is simply to disable Git in the command
prompt. Run the following to update Git's configuration in your working tree:

```
$ git config codespaces-theme.hide-status 1
```

See https://github.com/devcontainers/features/tree/main/src/common-utils for more details.
