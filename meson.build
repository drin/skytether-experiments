# ------------------------------
# Project Configuration

project(
     'experiment-arrowcompute'
    ,'cpp'
    ,version         : '0.1'
    ,default_options : ['warning_level=3', 'cpp_std=c++17']
)

add_project_arguments('-DDEBUG=0', language: 'cpp')


# ------------------------------
# Dependencies
dep_arrow = dependency('arrow-dataset', version: '>=15.0.0', static: false)


# ------------------------------
# Variables

src_dir_cpp = 'src' / 'cpp'

hdrlist_experiments = [
    src_dir_cpp / 'adapter_arrow.hpp'
   ,src_dir_cpp / 'util_arrow.hpp'
   ,src_dir_cpp / 'operators.hpp'
   ,src_dir_cpp / 'experiments.hpp'
]

srclist_experiments = [
   src_dir_cpp / 'adapter_arrow.cpp'
  ,src_dir_cpp / 'util_arrow.cpp'
  ,src_dir_cpp / 'operators.cpp'
]


# ------------------------------
# Executables

# >> Experiment 1
# source files to make the experiment executable
srclist_experiment1 = (
    [ src_dir_cpp / 'experiment_one.cpp' ]
  + srclist_experiments
)

bin_experiment1 = executable('experiment-arrowcompute'
    ,srclist_experiment1
    ,dependencies       : dep_arrow
    ,include_directories: src_dir_cpp
    ,install            : false
)
