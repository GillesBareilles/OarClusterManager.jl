# OarClusterManager.jl

A julia Cluster Manager implementation for an OAR cluster.

## Installation

```julia
]add https://github.com/GillesBareilles/OarClusterManager.jl.git
```

## Use

To add a process on nodes `node1`, `node2`, ...
```julia
addprocs_oar([node1, node2, ...])
```

`get_remotehosts()` returns the array of all other nodes, with multiplicity of reserved nodes.