using Serialization

const ATTACHMENT_BUFFER = Ref{Vector{Expr}}(Expr[])
const LAST_ATTACHMENT_IDX = Ref{Int}(0)

save_attachment(ex::Expr) = push!(ATTACHMENT_BUFFER[], ex)
get_attachments() = ATTACHMENT_BUFFER[]
clear_attachments!() = ATTACHMENT_BUFFER[] = Expr[]

function generate_temp_filename(prefix::AbstractString, suffix::AbstractString)
    LAST_ATTACHMENT_IDX[] = LAST_ATTACHMENT_IDX[] + 1
    attachment_idx = lpad(LAST_ATTACHMENT_IDX[], 8, '0')
    prefix * attachment_idx * suffix
end

function convert_to_uri(path::AbstractString)
    # add_file wants URI format in order to work in local mode
    # and it does seem to work unless we have three slashes at the statr
    if Sys.iswindows()
        "file:///" * replace(path, '\\' => '/')
    elseif startswith(path, "/")
        # absolute path
        "file://" * path
    else
        # relative path
        # "file:./" * path
        "file://" * abspath(path)
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
            add_file(sc, convert_to_uri(ex.args[2]))
        end
    end
    clear_attachments!()
end

macro attach(ex)
    save_attachment(ex)
    esc(ex)
end

"Makes the value of data available on workers as symbol name"
function share_variable(sc::SparkContext, name::Symbol, data::Any)
    temp_filename = generate_temp_filename("sharevar_", ".jbin")
    path = joinpath(get_temp_dir(sc), temp_filename)
    open(path, "w") do io
        serialize(io, data)
    end
    add_file(sc, convert_to_uri(path))
    ex = quote
            using Serialization
            open($temp_filename, "r") do io
                global $name = deserialize(io)
            end
         end
    save_attachment(ex)
    data
end

"Makes the variable available on workers"
macro share(sc, var)
    q = Expr(:quote, var)
    :(share_variable($(esc(sc)), $q, $(esc(var))))
end
