module OarClusterManager

using Distributed, DelimitedFiles

import Distributed: launch, manage, kill
export launch, manage, kill

export OARManager, addprocs_oar, get_remotehosts, get_ncoresmaster

struct OARManager <: ClusterManager
    machines::Dict

    function OARManager(machines)
        mhist = Dict()
        for m in machines
            current_cnt = get(mhist, m, 0)
            mhist[m] = current_cnt + 1
        end
        return new(mhist)
    end
end

"""
    get_remotehosts()

Return an array of all OAR reserved remote nodes, with multiplicity of number of available cores.
"""
function get_remotehosts()
    @assert haskey(ENV, "OAR_NODEFILE")

    allhosts = vec(readdlm(ENV["OAR_NODEFILE"], String))

    return filter(x->x!==gethostname(), allhosts)
end

"""
    get_ncoresmaster()

Return the number of allocated cores on master.
"""
function get_ncoresmaster()
    @assert haskey(ENV, "OAR_NODEFILE")

    allhosts = vec(readdlm(ENV["OAR_NODEFILE"], String))

    return count(x->x==gethostname(), allhosts)
end

function addprocs_oar(machines::AbstractVector; kwargs...)
    addprocs(OARManager(machines); kwargs...)
end

function launch(manager::OARManager, params::Dict, launched::Array, launch_ntfy::Condition)
    # Implemented by cluster managers. For every Julia worker launched by this function, it should append a WorkerConfig entry 
    # to launched and notify launch_ntfy. The function MUST exit once all workers, requested by manager have been launched. 
    # params is a dictionary of all keyword arguments addprocs was called with.

    launch_tasks = Vector{Any}(undef, sum(values(manager.machines)))
    task_id = 1
    for (machine, count) in manager.machines
        ## Start count processes on machine machine
        for cnt in 1:count
            launch_tasks[task_id] = @async try
                launch_on_machine(manager, machine, params, launched, launch_ntfy)
            catch e
                print(stderr, "exception launching on machine $(machine) : $(e)\n")
            end
            task_id += 1
        end
    end

    ## Wait for spawn processes to exit
    for t in launch_tasks
        wait(t)
    end
    
    notify(launch_ntfy)
    # println("Done launch().\n")
    return
end

function launch_on_machine(manager::OARManager, machine::String, params::Dict, launched::Array, launch_ntfy::Condition)
    printlev = 0
    printlev > 0 && println("\n-- launch_on_machine()")
    printlev > 0 && @show machine
    printlev > 0 && @show params
    printlev > 0 && @show launched
    printlev > 0 && @show launch_ntfy
    printlev > 0 && println("------")
    
    dir = params[:dir]
    exename = params[:exename]
    exeflags = params[:exeflags]

    # First cookie passing option
    # exeflags = `$exeflags --worker `

    # Second cookie passing option
    exeflags = `$exeflags --worker=$(cluster_cookie())`

    host = machine
    # oarshflags = `$(get(params, :oarshflags, \`\`))`

    # Build up the ssh command

    # the default worker timeout
    tval = get(ENV, "JULIA_WORKER_TIMEOUT", "")

    # Julia process with passed in command line flag arguments
    cmds = """
        cd -- $(Base.shell_escape_posixly(dir))
        $(isempty(tval) ? "" : "export JULIA_WORKER_TIMEOUT=$(Base.shell_escape_posixly(tval))")
        $(Base.shell_escape_posixly(exename)) $(Base.shell_escape_posixly(exeflags))"""

    # shell login (-l) with string command (-c) to launch julia process
    cmd = `sh -l -c $cmds`

    # remote launch with ssh with given ssh flags / host / port information
    cmd = `oarsh $host $(Base.shell_escape_posixly(cmd))`

    # launch the remote Julia process

    # detach launches the command in a new process group, allowing it to outlive
    # the initial julia process (Ctrl-C and teardown methods are handled through messages)
    # for the launched processes.
    printlev > 0 && println("Launching full command:\n$cmd\n")
    io = open(detach(cmd), "r+")
    
    ## Second cookie passing option.
    # write_cookie(io) = print(io.in, string(cluster_cookie(), "\n"))

    wconfig = WorkerConfig()
    wconfig.io = io.out
    wconfig.host = host
    # wconfig.tunnel = get(params, :tunnel, false)
    # wconfig.sshflags = ``
    wconfig.exeflags = exeflags
    wconfig.exename = exename
    wconfig.count = 1
    # wconfig.max_parallel = get(params, :max_parallel, 1)
    # wconfig.enable_threaded_blas = get(params, :enable_threaded_blas, false)


    push!(launched, wconfig)
    notify(launch_ntfy)
    
    printlev > 0 && @show wconfig.ospid
    printlev > 0 && @show launched
    printlev > 0 && println("Notif sent, returning.\n")
    return
end


function manage(manager::OARManager, id::Integer, config::WorkerConfig, op::Symbol)
    # Implemented by cluster managers. It is called on the master process, during a worker's lifetime, with appropriate op values:
    #   - with :register/:deregister when a worker is added / removed from the Julia worker pool.
    #   - with :interrupt when interrupt(workers) is called. The ClusterManager should signal the appropriate worker with an interrupt signal.
    #   - with :finalize for cleanup purposes.
    printlev = 0
    printlev > 0 && println("\nManage()")
    printlev > 0 && @show manager
    printlev > 0 && @show id
    printlev > 0 && @show config
    printlev > 0 && @show op

    printlev > 0 && @show config.ospid

    if op == :interrupt
	    ospid = config.ospid
	    if ospid !== nothing
		    host = config.host
		    if !success(`oarsh $host "kill -2 $ospid"`)
			    @error "Error sending a Ctrl-C to julia worker $id on $host"
		    end
	    else
		    # This state can happen immediately after an addprocs
		    @error "Worker $id cannot be presently interrupted"
	    end
    end

    printlev > 0 && println("Returning")
    return
end


end # module