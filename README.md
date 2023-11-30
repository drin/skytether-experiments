# Skytether Experiments

Consolidation of experiments for evaluating aspects of skytether and decomposable queries.


## Organization
### Source files
The repository should be organized in a (mostly) straightforward manner. Each experiment
is wholly encapsulated in a sub-directory. For each experiment, build files
(`meson.build`) and experiment-specific information are in the experiment directory and
all source and header files in a `src` sub-directory and test data are in a `resources`
sub-directory along with any other relevant assets.

###  LFS
The test data is in git LFS because I thought that may be convenient (apologies if it
isn't). So, be sure to get the test data after checking out the repository:

```bash
# just make sure LFS is installed
git lfs install

# downloads the specific file data from LFS
git lfs checkout "resources/E-GEOD-76312.48-2152.x565.feather"

# you can also pull all files in a given directory
# git lfs pull .
```

## Compilation
This repository uses [meson][web-meson] to compile. This is partially because I don't understand
CMake very well, and partially because I have found meson to be extremely easy to learn and use.

As of today, I am using meson 1.3.0. Meson is easy to install into a python environment,
which then provides a meson CLI command. I am not sure that system packages on non
rolling-release distros will have the latest version, but the meson build file,
`meson.build`, should be compatible with older versions. Note that meson uses ninja, so it
is likely you will need to install ninja before meson, and that familiarity with ninja
will make some meson commands easier to understand.

To actually build the code with meson, here are some simple bootstrap instructions:

```bash
# creates a directory, called `build-dir`
# do this in a directory containing a `meson.build` file
meson setup build-dir

# command to compile; the `-C` flag says to "change to the given directory and run the command"
# I think this is essentially ninja syntax
meson compile -C build-dir
```

Note that the ">> Dependencies" section of the meson.build file specifies our only "real"
dependency, Apache Arrow 12.0.1 (some arrow APIs may change between versions).


## Experiments

### Arrow Compute (in-memory)
See directory `arrow-compute-inmem`.


<!-- resources -->
[web-meson]: https://mesonbuild.com/
