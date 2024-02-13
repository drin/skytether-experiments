project(
     'experiment-arrowcompute'
    ,'cpp'
    ,version         : '0.1'
    ,default_options : ['warning_level=3', 'cpp_std=c++17']
)

add_project_arguments('-DDEBUG=0', language: 'cpp')


# >> Dependencies
dep_arrow = dependency('arrow-dataset', version: '>=15.0.0', static: false)


# >> Executables

# source files to make the experiment executable
experiment_srclist = [
   # main source file
   'src' / 'experiment.cpp'

   # dependency source files
  ,'src' / 'table_op.cpp'
  ,'src' / 'adapter_arrow.cpp'
]

exe_test = executable('experiment-arrowcompute'
    ,experiment_srclist
    ,install     : false
    ,dependencies: dep_arrow
)
