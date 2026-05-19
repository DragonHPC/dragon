# Contributing to Dragon

The Dragon team follows processes from architecture review through to code merge that are documented
here: [Dragon Processes](https://hpe.sharepoint.com/:p:/t/PE/EYHNRreNetJBgwiKcKNjqekBUoqDrKcLoSJyRggA8zomWA?e=x8laHx).
For development effort involving new features, you must refer to that presentation and follow our
process for design discussions and review prior to contributing any code.

For code development of any kind, we always start from the Jira ticket originating the work.
Create a branch from Jira off of master so the branch name can be easily tracked back to the
work description.  We always require pull requests with review before code can be merged into
master.  It is your repsonsibility to assign 2-3 reviewers on any of your PRs.  If you need
help identifying the right reviewers, ask the Dragon team lead.

## Contribution Workflow

While following the processes linked above, a typical workflow on any Linux box is as follows:

1. Clone the repository

```
git clone git@github.hpe.com:hpe/hpc-pe-dragon-dragon.git
cd hpc-pe-dragon-dragon
```

2. Checkout your working branch set by the Jira task you are working on

```
git checkout [branch from Jira]
```

3. Setup your environment.

Dragon requires GCC version >= 9 and Python versions 3.10 - 3.12. Without these version requirements,
the code will not build. Assuming these requirements are met, we can proceed.

The very first time you want to build after cloning the repository, you'll want to do a
`clean_build` to set up the Python environment and get it all built. This sets up
environment variables for a development build (eg: `DRAGON_BASE_DIR`, `PATH`, etc.).
However, it does not configure support for the HSTA. For that info, please refer
to "Configuring the Build for the High Speed Transport Agent (HSTA)" section of the
`README.md`

```
. hack/clean_build
```

After doing this once, on subsequent builds you can use the `build` helper script to build dragon

```
. hack/build
```

If you have already built it and just want to run something, then you can set up the
environment with

```
. hack/setup
```

If things aren't working and you need a fresh build you can try a clean build again with

```
. hack/clean_build
```

4. If you are running a container, base requirements are met, but for setting up variables you can do the following
steps to build in a container.

```
cd src && make distclean && make
```

and to rebuild in the container you can use

```
cd src && make
```


5. Make code, test, and documentation modification as-needed for your Jira task

6. Run the unit tests and verify all tests pass

```
cd ../test
make test
```

7. Verify the documentation builds correctly if you made documentation changes

```
cd ../doc
make
```

8. Commit your changes locally

```
git commit -m "enter a useful message here about the commit" [changed files]
```

9. Push your changes to origin

```
git push
```

A pipeline will be kicked off now on your branch that builds and runs unit tests.  You can
access the pipeline status at [Jenkins Pipeline](https://cje.dev.cray.com/teams-pe-team/blue/organizations/pe-team/PE-Dragon%20Sles15sp1%20Github%2Fhpc-pe-dragon-dragon/activity).  Be sure your branch is passing
through the pipeline successfully before going on to the next step.

10. Start a pull request review on Github

Assign at least 2 reviewers who are familiar with the work in your Jira task.  You should
not merge the pull request until you have approvals from all reviewers.  Please be
considerate of reviewers.  Do not put up a pull request that hasn't passed all checks.

## src/ Directoty make Targets

The `src/Makefile` has a number of targets that might be of interest.  They are as follows:

| Target             | Purpose                                                                     |
|:-------------------|-----------------------------------------------------------------------------|
| build              | (default target) Build Dragon components                                    |
| dist               | Build Dragon components if-needed and produce a package for release         |
| clean              | Clean all products from `dist` but do not clean Dragon components           |
| distclean          | Clean all products from `dist` and all Dragon components                    |
| install-pythondeps | Do user pip install of required Python packages for tests and documentation |

## Additional Tools

There is a `hack` directory at the top-level where additional helper scripts can be placed.  Anyone
adding scripts there is responsible for maintaining them.  The scripts are:

| Script       | Purpose                                                                          |
|:-------------|----------------------------------------------------------------------------------|
| setup        | File to source that runs two module commands needed to setup a build environment |
| dragonbuild  | Allows you to be in any directory and rebuild Dragon                             |
| dragonclean  | Cleans up /dev/shm and logfiles from Dragon                                      |
| where4all    | Batch script that executes script.gbd on processes & threads to get their status |

## Dev Container Development

### Setup

If using VS Code, the [Remote Containers extension] supports opening your
working tree in a container. Run the **Remote-Containers: Open Folder in
Container...** command from the Command Palette (`F1`) or quick actions Status
bar item, and select your Dragon project folder. VS Code automatically builds
the container image and runs it based on the configuration in
.devcontainer/devcontainer.json.

The dev container image (see .devcontainer/Dockerfile) is based on Ubuntu and
does not include any Cray-specific repositories or packages. However, it
includes appropriate versions of required tools to build and test Dragon as well
as the documentation. In particular, Python is built from source using [pyenv],
and it's source is available in /usr/local/src.

> Note that VS Code is not required to build the image, but it is recommended.
> To build using Docker directly, e.g.:
>
> ```
> $ docker build -t dragon-dev .devcontainer
> ```
>
> As long as the cache is used when building the image, it will result in the
> same image ID as the one built by VS Code, e.g.:
>
> ```
> $ docker images
> REPOSITORY                                                  TAG          IMAGE ID       CREATED        SIZE
> dragon-dev                                                  latest       c37896698c3f   25 hours ago   2.23GB
> vsc-hpc-pe-dragon-dragon-ec2b3104eaef710f570ebd5fd48d2534   latest       c37896698c3f   25 hours ago   2.23GB
> ```

Once VS Code has opened your folder in the dev container, any terminal window
you open in VS Code (**Terminal > New Terminal**) will automatically run a Bash
interactive login shell in the VSCode container rather than locally. The dev container
configuration refreshes the Git index since the host OS is most likely not Linux, see
https://stackoverflow.com/questions/62157406/how-to-share-a-git-index-between-2-oses-avoid-index-refresh
and
https://stackoverflow.com/questions/59061816/git-forces-refresh-index-after-switching-between-windows-and-linux
for more information.

> To build or test Dragon, you will need to properly initialize the environment
> by setting development environment variables
>
> ```
> $ docker run --rm -ti -u "$(id -u):$(id -g)" -v "$(pwd):/dragon" dragon-dev   # Note: this is typically done for you via VSCode
> / $ . hack/setup   # This needs to be run to set up your development environment anytime you're doing development for Dragon
> ```

Note that for convenience VS Code remaps the root user in the
container (UID 0) to your local user e.g.:

```
root ➜ /workspaces/hpc-pe-dragon-dragon (ubuntu-dev-container ✗) $ whoami
root
root ➜ /workspaces/hpc-pe-dragon-dragon (ubuntu-dev-container ✗) $ id
uid=0(root) gid=0(root) groups=0(root)
root ➜ /workspaces/hpc-pe-dragon-dragon (ubuntu-dev-container ✗) $ ls -l
total 24
-rw-r--r--   1 root root 11846 Mar 29 21:03 CONTRIBUTING.md
drwxr-xr-x   6 root root   192 Mar 29 21:03 demo
drwxr-xr-x  11 root root   352 Mar 29 21:03 doc
drwxr-xr-x   5 root root   160 Mar 29 21:03 dst
drwxr-xr-x   5 root root   160 Mar 29 21:03 examples
drwxr-xr-x   6 root root   192 Mar 29 21:04 external
drwxr-xr-x   5 root root   160 Mar 29 21:04 hack
-rw-r--r--   1 root root   200 Mar 29 15:05 Jenkinsfile.sle15sp1
-rw-r--r--   1 root root  6213 Mar 29 21:03 README.md
drwxr-xr-x  22 root root   704 Mar 30 14:33 src
drwxr-xr-x 119 root root  3808 Mar 29 21:04 test
```

> When running a dev container using Docker directly, it's usually sufficient to
> specify the effective user via `-u "$(id -u):$(id -g)"` in order to ensure
> consistent file system permissions. E.g.:
>
> ```
> $ docker run --rm -ti -u "$(id -u):$(id -g)" -v "$(pwd):/dragon" dragon-dev
> / $ whoami
> whoami: cannot find name for user ID 503
> / $ id
> uid=503 gid=12790 groups=12790
> / $ ls -l dragon
> total 8
> -rw-r--r--  1 root root  885 Feb  2 16:06 README.md
> drwxr-xr-x 12 root root  384 Feb  2 16:06 demo
> drwxr-xr-x  4 root root  128 Feb  7 15:55 external
> -rwxr-xr-x  1 root root 1373 Feb  2 16:06 setup.sh
> drwxr-xr-x 30 root root  960 Feb  8 15:41 src
> drwxr-xr-x 54 root root 1728 Feb  8 22:34 test
> ```

### Build

Once you have a dev container running, you can build as you normally
would using `make`:

```
root ➜ /workspaces/hpc-pe-dragon-dragon (ubuntu-dev-container ✗) $ cd src
root ➜ /workspaces/hpc-pe-dragon-dragon/src (ubuntu-dev-container ✗) $ make
```

### Test

By default, VS Code appropriately initializes the environment in order to run
tests, e.g.:

```
root ➜ /workspaces/hpc-pe-dragon-dragon (ubuntu-dev-container ✗) $ cd test
root ➜ /workspaces/hpc-pe-dragon-dragon/test (ubuntu-dev-container ✗) $ make
```

> **Warning:** Although Python multiprocessing targets `mp_unittest_dragon` and
> `mp_unittest_udragon` have been updated to use the unit tests included with
> the installed Python version, there still appears to be issues with
> successfully swapping start methods which impact testing. In addition, the
> `score_card_` targets remain untested.


### Python Formatting

The Black python package is used for formatting and adhering to PEP8 standards,
with a line length set to 120 to accomodate longer naming schemes.  Black can
either be integrated directly with your editor, or run from the terminal, and
should automatically pick up on the `pyproject.toml` file for formatting rules.

If certain files have sensitive formatting and need to be excluded, they can be
added to the `pyproject.toml` file under the `force-exclude` flag to prevent
automatic format changes.

If you use an editor not listed below, please feel free to add instructions on
integrating Black into the workflow.

**NOTE:** Line length limits will not apply to excessively long strings,
or strings that have inline formatting (e.g. `print(f"Foo is {foo}")`), nor
comment lines that exceed the specified line length.

If you want to specifically prevent Black from reformatting something into a
single line, you can use the "magic trailing comma".  Just leave a dangling
comma at the end of the function call or array arguments, and it will be
stacked instead of condensed into one line.  Example:

```
x = foo(bar=bar, baz=baz, buzz=buzz)
```

With a magic comma, becomes
```
x = foo(
    bar=bar,
    baz=baz,
    buzz=buzz,
)
```

## VS Code

Install the Black VSCode Extension [VSCode-black].

Open settings, and search "python format".
Look for "Default Formatter" and from the drop-down, select Black.

Still in settings, search "format on save".  Enable this, and set
"Editor: Format on Save Mode" to "File".

Black will do the rest and recursively search for a `pyproject.toml` file for rules.

As a suggestion, adding a ruler mark at 120 lines may also be useful.  To do this,
open `settings.json` and add the following:

```
"editor.rulers": [
    120
]
```

An example of general-use `settings.json` for VSCode is as follows:

```
"files.trimTrailingWhitespace": true,
"editor.defaultFormatter": "ms-python.black-formatter",
"editor.formatOnSave": true,
"editor.rulers": [
    120
]
```


[VS Code Remote Containers]: https://code.visualstudio.com/docs/remote/containers
[Remote Containers extension]: https://code.visualstudio.com/docs/remote/containers-tutorial
[pyenv]: https://github.com/pyenv/pyenv
[VSCode-black]: https://marketplace.visualstudio.com/items?itemName=ms-python.black-formatter