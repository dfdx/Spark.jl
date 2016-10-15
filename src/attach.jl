
const ATTACHMENT_BUFFER = Ref{Vector{Expr}}(Expr[])
const LAST_ATTACHMENT_IDX = Ref{Int}(0)

save_attachment(ex::Expr) = push!(ATTACHMENT_BUFFER[], ex)
get_attachments() = ATTACHMENT_BUFFER[]
clear_attachments!() = ATTACHMENT_BUFFER[] = Expr[]

function process_attachments(sc::SparkContext)
    mktempdir() do dirpath
        for ex in get_attachments()
            LAST_ATTACHMENT_IDX[] = LAST_ATTACHMENT_IDX[] + 1
            attachment_idx = @sprintf "%08d" LAST_ATTACHMENT_IDX[]
            path = joinpath(dirpath, "attached_" * attachment_idx * ".jl")
            open(path, "w") do io
                write(io, string(ex))
            end
            add_file(sc, path)
            # if it's `include` expression, also attach included file
            if isa(ex, Expr) && ex.head == :call && ex.args[1] == :include
                add_file(sc, ex.args[2])
            end
         end
    end
    clear_attachments!()
end

macro attach(ex)
    save_attachment(ex)
    esc(ex)
end
