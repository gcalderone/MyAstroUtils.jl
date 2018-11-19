module Astrocook

using DataFrames, FITSIO, AstroLib, SkyCoords, StructC14N, SortMerge, WCS

import Base.parse, Base.convert, Base.join


parse(::Type{FK5Coords{2000, T1}}, ss::T2) where {T1 <: AbstractFloat, T2 <: AbstractString} =
    parse(FK5Coords{2000, T1}, tuple(split(ss)...))

function parse(::Type{FK5Coords{2000, T1}}, ss::NTuple{2, T2}) where {T1 <: AbstractFloat, T2 <: AbstractString}
    function string2angle(ss::T) where T <: AbstractString
        s = strip(ss)
        sign = 1.
        (s[1] == '-')  &&  (s = s[2:end]; sign = -1.)
        (s[1] == '+')  &&  (s = s[2:end])
        
        if occursin(":", s)
            @assert length(s) > 8 "Invalid string input"
            d1 = Meta.parse(s[1:2])
            d2 = Meta.parse(s[4:5])
            d3 = Meta.parse(s[7:end])
        else
            @assert length(s) > 6 "Invalid string input"
            d1 = Meta.parse(s[1:2])
            d2 = Meta.parse(s[3:4])
            d3 = Meta.parse(s[5:end])
        end
        return sign * (d1 + d2/60. + d3/3600.)
    end
    ra_deg = string2angle(strip(ss[1])) * 15.
    de_deg = string2angle(strip(ss[2]))
    return SkyCoords.FK5Coords{2000,T1}(ra_deg, de_deg)
end



function convert(::Type{DataFrame}, hdu::FITSIO.TableHDU)
    fld = FITSIO.colnames(hdu)
    out = DataFrame()
    for i in 1:length(fld)
        tmp = read(hdu, fld[i])
        if ndims(tmp) == 1
            out[Symbol(fld[i])] = tmp
        else
            for j in 1:(size(tmp))[1]
                out[Symbol(fld[i] * string(j))] = tmp[j,:]
            end
        end
    end
    return out
end


function convert(::Type{DataFrame}, a::Matrix)
    df = DataFrame()
    (nrow, ncol) = size(a)
    for i in 1:ncol
        df[Symbol("c", i)] = a[:,i]
    end
    return df
end

function join(ra1::Vector{T1}, de1::Vector{T1}, ra2::Vector{T1}, de2::Vector{T1}, thresh_asec::T2; sorted=false) where
    {T1 <: AbstractFloat, T2 <: AbstractFloat}
    lt(v, i, j) = ((v[i, 2] - v[j, 2]) < 0)
    function sd(c1, c2, i1, i2, thresh_asec)
        thresh_deg = thresh_asec / 3600. # [deg]
        dd = c1[i1, 2] - c2[i2, 2]
        (dd < -thresh_deg)  &&  (return -1)
        (dd >  thresh_deg)  &&  (return  1)
        dd = gcirc(2, c1[i1, 1], c1[i1, 2], c2[i2, 1], c2[i2, 2])
        (dd <= thresh_asec)  &&  (return 0)
        return 999
    end
    return sortmerge([ra1 de1], [ra2 de2], thresh_asec, lt1=lt, lt2=lt, sd=sd, sorted=sorted)
end


end # module
