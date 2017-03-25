
const ATTACHMENT_BUFFER = Ref{Vector{Expr}}(Expr[])
const LAST_ATTACHMENT_IDX = Ref{Int}(0)

save_attachment(ex::Expr) = push!(ATTACHMENT_BUFFER[], ex)
get_attachments() = ATTACHMENT_BUFFER[]
clear_attachments!() = ATTACHMENT_BUFFER[] = Expr[]

function generate_temp_filename(prefix::AbstractString, suffix::AbstractString)
    LAST_ATTACHMENT_IDX[] = LAST_ATTACHMENT_IDX[] + 1
    attachment_idx = @sprintf "%08d" LAST_ATTACHMENT_IDX[]
    prefix * attachment_idx * suffix
end

function convert_to_uri(path::AbstractString)
    # add_file wants URI format in order to work in local mode
    # and it does seem to work unless we have three slashes at the statr
    if is_windows()
        "file:///" * replace(path, r"\\", s"/")
    else
        "file://" * path
    end
end

function process_attachments(sc::SparkContext)
    for ex in get_attachments()
        temp_filename = generate_temp_filename("attached_", ".jl")
        path = joinpath(get_temp_dir(sc), temp_filename)
        open(path, "w") do io
            write(io, string(ex))
        end
        
        add_file(sc, convert_to_uri(path))

        # if it's `include` expression, also attach included file
        if isa(ex, Expr) && ex.head == :call && ex.args[1] == :include
            add_file(sc, ex.args[2])
        end
    end
    clear_attachments!()
end

macro attach(ex)
    save_attachment(ex)
    esc(ex)
end

function share_variable_internal(sc::SparkContext, var::Any, var_name::AbstractString)
    temp_filename = generate_temp_filename("sharevar__", ".jbin")
    path = joinpath(get_temp_dir(sc), temp_filename)
    open(path, "w") do io
        serialize(io, var)
    end
    add_file(sc, convert_to_uri(path))
    worker_string = """
            open(\"$temp_filename\", \"r\") do io
                global $var_name = deserialize(io)
            end
            """
    save_attachment(parse(worker_string))
end

# the string function doesn't work correctly inside quotes
# so need this extra call so that we get the right variable name
macro variable_name(var)
    string(var)
end

"Makes the variable available on workers"
macro share_variable(sc, var)
    quote
        share_variable_internal($(esc(sc)), $(esc(var)), @variable_name($(esc(var))))
    end
end
