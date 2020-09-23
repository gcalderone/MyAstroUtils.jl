using DataFrames, FITSIO, ProgressMeter

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
    data = Array{Any}(undef, 0)
    for name in names(dfr)
        if eltype(dfr[:, name]) == Symbol
            push!(data, string.(dfr[:,name]))
        elseif eltype(dfr[:, name]) == String
            push!(data, string.(dfr[:,name]))
        else
            push!(data, dfr[:,name])
        end
    end
    write(f, string.(names(dfr)), data)
end
