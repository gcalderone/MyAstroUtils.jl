using DataFrames

gaussian(x, mean=0., sigma=1.) =
    (1 / sqrt(2pi) / sigma) * exp(-((x - mean) / sigma)^2 / 2)

showv(df::DataFrameRow) =
    show(DataFrame(field=names(df), value=[values(df)...]), allrows=true, allcols=true)

function askpass(msg="")
    @assert Base.Sys.isunix()
    io = Base.getpass(msg)
    out = String(read(io))
    Base.shred!(io)
    return out
end


function compare_df(a::DataFrame, b::DataFrame)
    if ncol(a) != ncol(b)
        @warn "Number of cols is different: " ncol(a) ncol(b)
    end
    if nrow(a) != nrow(b)
        @warn "Number of rows is different: " nrow(a) nrow(b)
    end
    na = names(a)
    nb = names(b)

    function sd(v1, v2, i1, i2)
        if v1[i1] < v2[i2]
            return -1
        elseif v1[i1] == v2[i2]
            return 0
        end
        return 1
    end
    jj = sortmerge(na, nb, sd=sd)
    if  (length(jj[1]) < length(na))  ||
        (length(jj[2]) < length(nb))
        @warn "Only " * string(length(jj[1])) * " columns have the same name"
    end

    if nrow(a) != nrow(b)
        @warn "Number of rows is different"
    else
        for i in 1:length(jj[1])
            colname = string(na[jj[1][i]])
            @info colname
            da = a[:, jj[1][i]]
            db = b[:, jj[2][i]]

            k = findall(ismissing.(da) .!= ismissing.(db))
            if length(k) != 0
                @warn string(length(k)) * " rows have different missing condition on column $colname"
            else
                if count(.!ismissing.(da)) > 0
                    da = disallowmissing(collect(skipmissing(da)))
                    db = disallowmissing(collect(skipmissing(db)))

                    (length(da) == 0)  &&  continue
                    if eltype(da) <: AbstractFloat
                        k = findall(da .!== db)  # handle NaNs
                    else
                        k = findall(da .!= db)  # handle NaNs
                    end
                    if length(k) != 0
                        @warn string(length(k)) * " rows have different values on column $colname"
                    end
                end
            end
        end
    end
end


function strip_blanks!(df::DataFrame)
    for i in 1:ncol(df)
        if nonmissingtype(eltypes(df)[i]) == String
            df[:, i] .= string.(strip.(df[:, i]))
        end
    end
end
