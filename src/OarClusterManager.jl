module OarClusterManager

using Distributed

import Distributed: launch, manage, kill
export launch, manage, kill

export OARManager, addprocs_oar

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
    println("Done launch().\n")
    return
end

function launch_on_machine(manager::OARManager, machine::String, params::Dict, launched::Array, launch_ntfy::Condition)
    println("\n-- launch_on_machine()")
    @show machine
    @show params
    @show launched
    @show launch_ntfy
    println("------")
    
    dir = params[:dir]
    exename = params[:exename]
    exeflags = params[:exeflags]


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
    println("Launching full command:\n$cmd\n")
    prcs = detach(cmd)
    println("------------------------")
    io = open(prcs, "r+")
    # print(io, cluster_cookie())

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
    
    @show wconfig.ospid
    @show launched
    println("Notif sent, returning.\n")
    return
end


function manage(manager::OARManager, id::Integer, config::WorkerConfig, op::Symbol)
    # Implemented by cluster managers. It is called on the master process, during a worker's lifetime, with appropriate op values:
    #   - with :register/:deregister when a worker is added / removed from the Julia worker pool.
    #   - with :interrupt when interrupt(workers) is called. The ClusterManager should signal the appropriate worker with an interrupt signal.
    #   - with :finalize for cleanup purposes.
    println("\nManage()")
    @show manager
    @show id
    @show config
    @show op

    @show config.ospid

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

    println("Returning")
    return
end


end # module