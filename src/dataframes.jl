using PrettyTables
export writetdf, readtdf, add_column_with_missing!


add_column_with_missing!(df::DataFrame, colname, ::Val{T}) where T <: Real = add_column_with_missing!(df, colname, T(0))
add_column_with_missing!(df::DataFrame, colname, ::Val{String})            = add_column_with_missing!(df, colname, "")
add_column_with_missing!(df::DataFrame, colname, ::Val{Symbol})            = add_column_with_missing!(df, colname, :a)

function add_column_with_missing!(df::DataFrame, colname, value::T) where T
    @assert !(T <: AbstractVector)
    @assert !ismissing(value)
    df[!, colname] .= fill(value, nrow(df))
    allowmissing!(df, colname)
    df[:, colname] .= missing
    return df
end


function writetdf(filename, df)
    out = DataFrame()
    align = Symbol[]
    @assert nrow(df) > 1
    for name in Symbol.(names(df))
        col = deepcopy(df[:, name])
        t0 = eltype(col)
        tt = nonmissingtype(t0)
        ii = findall(.!ismissing.(col))
        if tt <: AbstractString
            col[ii] .= String.(strip.(df[ii, name]))
            push!(align, :l)
        elseif (tt == Bool)  ||
            (tt == UnitRange{Int64})  ||
            (tt == StepRange{Int64, Int64})
            ;
            push!(align, :c)
        elseif tt <: Integer
            col[ii] .= Int64.(df[ii, name])
            push!(align, :r)
        elseif tt <: AbstractFloat
            col[ii] .= Float64.(df[ii, name])
            push!(align, :r)
        else
            error("Unsupported data type: $(t0)")
        end
        out[!, name] = col
    end

    f = open(filename, "w")
    pretty_table(f, out, tf=tf_ascii_rounded, alignment=align)
    close(f)
end


function readtdf(filename)
    lines = readlines(filename)
    names = Symbol.(strip.(split(lines[2], '│'))); names = names[2:end-1]
    types = String.(strip.(split(lines[3], '│'))); types = types[2:end-1]
    out = DataFrame()
    for icol in 1:length(names)
        tt = types[icol]
        if tt[end] == '?'
            out[!, names[icol]] = eval(Meta.parse("Union{Missing, " * tt[1:end-1] * "}[]"))
        else
            out[!, names[icol]] = eval(Meta.parse(tt * "[]"))
        end
    end
    for i in 5:length(lines)-1
        row = []
        line = String.(strip.(split(lines[i], '|')));
        line = line[2:end-1]

        for icol in 1:length(names)
            t0 = types[icol]
            tt = types[icol]
            hasmissing = false
            if tt[end] == '?'
                hasmissing = true
                tt = tt[1:end-1]
            end
            vv = line[icol]

            if hasmissing  &&  ((vv == "missing")  ||  (vv == ""))
                push!(row, missing)
            elseif tt == "String"
                push!(row, vv)
            elseif tt == "Bool"
                if (vv == "true")  ||  ((length(vv) != 0)  &&  (vv != "false"))
                    push!(row, true)
                else
                    push!(row, false)
                end
            elseif (tt == "UnitRange{Int64}")  ||
                (tt == "StepRange{Int64, Int64}")
                push!(row, eval(Meta.parse(vv)))
            elseif tt == "Int64"
                push!(row, Meta.parse(vv))
            elseif tt == "Float64"
                if vv == "NaN"
                    push!(row, NaN)
                else
                    push!(row, Meta.parse(vv))
                end
            else
                error("Unsupported data type: $(t0)")
            end
        end
        push!(out, row)
    end
    return out    
end
