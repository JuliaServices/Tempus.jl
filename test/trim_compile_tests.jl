using Test

const _TRIM_SUPPORTED = VERSION >= v"1.12.0-rc1"
const _TRIM_PRE_RELEASE = !isempty(VERSION.prerelease)
const _TRIM_SETUP_TIMEOUT_S = Sys.iswindows() ? 600.0 : 180.0
const _TRIM_COMPILE_TIMEOUT_S = Sys.iswindows() ? 600.0 : 300.0
const _TRIM_EXECUTABLE_TIMEOUT_S = Sys.iswindows() ? 120.0 : 30.0
const _JULIAC_ENTRYPOINT_EXPR =
    "using JuliaC; if isdefined(JuliaC, :main); JuliaC.main(ARGS); else JuliaC._main_cli(ARGS); end"

_trim_enabled() = get(ENV, "TEMPUS_RUN_TRIM_COMPILE", "1") == "1"

function _clean_cmd(cmd::Cmd)
    env = Dict{String,String}(key => value for (key, value) in ENV if key != "JULIA_LOAD_PATH")
    return setenv(cmd, env)
end

function _run_command_with_timeout(cmd::Cmd; timeout_s::Float64, log_label::String)
    output_path = tempname()
    output_stream = open(output_path, "w")
    exit_code = -1
    timed_out = false
    try
        process = run(pipeline(ignorestatus(cmd), stdout=output_stream, stderr=output_stream); wait=false)
        started_at = time()
        next_log_at = started_at + 10.0
        while Base.process_running(process)
            now = time()
            if now - started_at >= timeout_s
                timed_out = true
                try
                    kill(process)
                catch
                end
                break
            end
            if now >= next_log_at
                println("[trim] $(log_label) WAIT $(round(now - started_at; digits=1))s")
                flush(stdout)
                next_log_at = now + 10.0
            end
            sleep(0.1)
        end
        try
            wait(process)
        catch
        end
        exit_code = something(process.exitcode, -1)
    finally
        close(output_stream)
    end
    output = try
        read(output_path, String)
    finally
        rm(output_path; force=true)
    end
    return exit_code, output, timed_out
end

function _setup_trim_environment()
    package_path = normpath(joinpath(@__DIR__, ".."))
    environment_path = mktempdir()
    julia = joinpath(Sys.BINDIR, Base.julia_exename())
    setup = "import Pkg; Pkg.develop(path=$(repr(package_path))); Pkg.add([\"AbstractStores\", \"JSON\", \"JuliaC\"])"
    command = _clean_cmd(`$julia --startup-file=no --history-file=no --project=$environment_path -e $setup`)
    exit_code, output, timed_out = _run_command_with_timeout(
        command;
        timeout_s=_TRIM_SETUP_TIMEOUT_S,
        log_label="setup",
    )
    if exit_code != 0 || timed_out
        println(output)
        error("failed to set up trim test environment")
    end
    return environment_path
end

function _trim_verify_totals(output::String)
    summary = match(r"Trim verify finished with\s+(\d+)\s+errors,\s+(\d+)\s+warnings\.", output)
    if summary !== nothing
        return parse(Int, summary.captures[1]), parse(Int, summary.captures[2])
    end
    errors = length(collect(eachmatch(r"Verifier error #\d+:", output)))
    warnings = length(collect(eachmatch(r"Verifier warning #\d+:", output)))
    return errors, warnings
end

function _run_trim_case(project_path::String)
    script_path = joinpath(@__DIR__, "tempus_trim_state.jl")
    julia = joinpath(Sys.BINDIR, Base.julia_exename())
    mktempdir() do directory
        output_name = Sys.iswindows() ? "tempus_trim.exe" : "tempus_trim"
        command = _clean_cmd(`$julia --startup-file=no --history-file=no --project=$project_path -e $(_JULIAC_ENTRYPOINT_EXPR) -- --output-exe $output_name --project=$project_path --experimental --trim=safe $script_path`)
        cd(directory) do
            exit_code, output, timed_out = _run_command_with_timeout(
                command;
                timeout_s=_TRIM_COMPILE_TIMEOUT_S,
                log_label="compile",
            )
            errors, warnings = _trim_verify_totals(output)
            if exit_code != 0 || timed_out || errors != 0 || warnings != 0
                println(output)
            end
            @test !timed_out
            @test errors == 0
            @test warnings == 0
            @test exit_code == 0

            executable = joinpath(directory, output_name)
            @test isfile(executable)
            exit_code == 0 && isfile(executable) || return nothing
            run_exit, run_output, run_timed_out = _run_command_with_timeout(
                `$(abspath(executable))`;
                timeout_s=_TRIM_EXECUTABLE_TIMEOUT_S,
                log_label="run",
            )
            run_exit == 0 || println(run_output)
            @test !run_timed_out
            @test run_exit == 0
        end
    end
    return nothing
end

@testset "Trim compile" begin
    if !_trim_enabled()
        println("[trim] skip: TEMPUS_RUN_TRIM_COMPILE != 1")
        @test true
    elseif Sys.WORD_SIZE != 64
        println("[trim] skip non-64-bit Julia")
        @test true
    elseif Sys.iswindows()
        println(
            "[trim] skip Windows: Base.Artifacts override discovery used by " *
            "TimeZones is not trim-safe on stock Julia",
        )
        @test true
    elseif !_TRIM_SUPPORTED
        println("[trim] skip Julia < 1.12")
        @test true
    elseif _TRIM_PRE_RELEASE
        println("[trim] skip prerelease Julia")
        @test true
    else
        _run_trim_case(_setup_trim_environment())
    end
end
