using DataFrames, FITSIO, ProgressMeter, Dates

import DataFrames.DataFrame, Base.write

export fits2df

DataFrame(hdu::FITSIO.ImageHDU) = error("Can not convert an ImageHDU extension to a DataFrame")
function DataFrame(hdu::FITSIO.HDU)
    fld = join.(split.(FITSIO.colnames(hdu), '.'), '_')
    out = DataFrame()
		@showprogress 0.5 for i in 1:length(fld)
        tmp = read(hdu, (FITSIO.colnames(hdu))[i])
        if ndims(tmp) == 1
            out[!, Symbol(fld[i])] = tmp
        else
            app = DataFrame(Symbol(fld[i])=>Vector{typeof(tmp[1,1])}[])
            for j in 1:(size(tmp))[2]
                push!(app, [tmp[:,j]])
            end
            out = hcat(out, app)
        end
    end
    return out
end


function fits2df(filename::String, hdu=2)
    f = FITS(filename)
    out = DataFrame(f[hdu])
    close(f)
    return out
end


function write(f::FITSIO.FITS, dfr::DataFrame)
    df = deepcopy(dfr)
    data = Array{Any}(undef, 0)
    for name in names(df)
        tt = eltype(df[:, name])
        if isa(tt, Union)
            tt = nonmissingtype(tt)
            if tt == String
                @warn "Using empty string as missing value in field $name"
                df[ismissing.(df[:, name]), name] .= ""
            elseif tt <: AbstractFloat
                @warn "Using NaN as missing value in field $name"
                df[ismissing.(df[:, name]), name] .= NaN
            elseif tt <: Integer
                @warn "Using zero as missing value in field $name"
                df[ismissing.(df[:, name]), name] .= 0
            end
            disallowmissing!(df, name)
        end
        tt = eltype(df[:, name])
        @assert !isa(tt, Union)

        if tt == Symbol
            push!(data, string.(df[:, name]))
        elseif tt == String
            push!(data, string.(df[:, name]))
        elseif tt <: Dates.AbstractTime
            push!(data, string.(df[:, name]))
        elseif tt <: UInt64
            @warn "Converting UInt64 to Int64 for field $name"
            push!(data, Int64.(df[:, name]))
        else
            push!(data, df[:, name])
        end
    end
    write(f, string.(names(df)), data)
end
