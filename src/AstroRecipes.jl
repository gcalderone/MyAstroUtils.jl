module AstroRecipes

using DataFrames, FITSIO, AstroLib, SkyCoords, StructC14N, SortMerge, WCS

import Base.parse, Base.convert, Base.join, Base.write

export columnranges, parsefixedwidth, parsecatalog, substMissing, showrec

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



convert(::Type{DataFrame}, hdu::FITSIO.ImageHDU) = error("Can not convert an ImageHDU extension to a DataFrame")
function convert(::Type{DataFrame}, hdu::FITSIO.HDU)
    fld = join.(split.(FITSIO.colnames(hdu), '.'), '_')
    out = DataFrame()
    for i in 1:length(fld)
        tmp = read(hdu, (FITSIO.colnames(hdu))[i])
        if ndims(tmp) == 1
            out[!, Symbol(fld[i])] = tmp
        else
            for j in 1:(size(tmp))[1]
                out[!, Symbol(fld[i] * string(j))] = tmp[j,:]
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

function convert(::Type{DataFrame}, a::Tuple)
    df = DataFrame()
    ncol = length(a)
    for i in 1:ncol
        df[!, Symbol("c", i)] = a[i]
    end
    return df
end

function write(f::FITSIO.FITS, dfr::DataFrame)
    #data = Dict{String, Any}()
    #for name in names(dfr)
    #    data[string(name)] = dfr[name]
    #end

    data = Array{Any}(undef, 0)
    for name in names(dfr)
         push!(data, dfr[:,name])
    end
    write(f, string.(names(dfr)), data)
end


function join(ra1::Vector{T1}, de1::Vector{T1}, ra2::Vector{T2}, de2::Vector{T2}, thresh_asec::T3; sorted=false) where
    {T1 <: AbstractFloat, T2 <: AbstractFloat, T3 <: AbstractFloat}
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

function columnranges(sdesc::Vector{T}) where T <: AbstractString
    count = 0
    pos = Vector{Int}()
    chr = Vector{Char}()
    for line in sdesc
        while length(pos) < length(line)
            push!(pos, count)
            push!(chr, line[length(pos)])
        end
        count += 1
        for i in 1:length(line)
            if chr[i] == line[i]
                pos[i] += 1
            end
        end
    end
    pos = findall(pos .== count)
    (pos[  1] == 1)            ||  (prepend!(pos, 1))
    (pos[end] == length(chr))  ||  (push!(pos, length(chr)))

    act = Vector{Int}()
    for i in 1:length(pos)-1
        if pos[i]+1 != pos[i+1]
            push!(act, pos[i])
        end
    end
    (act[end] == pos[end])  ||  (push!(act, pos[end]))

    out = Vector{UnitRange{Int}}()
    for i in 1:length(act)-1
        push!(out, UnitRange{Int}(act[i], act[i+1]-1))
    end
    return out
end


parsefixedwidth(input::Vector{String}) = parsefixedwidth(input, columnranges(input))
function parsefixedwidth(input::Vector{String}, ranges::Vector{UnitRange{Int}})
    data = Vector{Vector{String}}(undef, length(ranges))
    for i in 1:length(ranges)-1
        data[i] = getindex.(input, Ref(ranges[i]))
    end
    tmp = Vector{String}(undef, length(input))
    for i in 1:length(input)
        tmp[i] = input[i][ranges[end][1]:end]
    end
    data[end] = tmp
    return data
end


function parsecatalog(sdesc::Vector{String}, input::Vector{String})
    # Parse description
    sdesc = sdesc[findall(strip.(sdesc) .!= "")]
    data = parsefixedwidth(sdesc)

    ranges = Vector{UnitRange{Int}}()
    for l in data[1]
        r = split(l, '-')
        (length(r) == 1)  &&  (r = [r[1], r[1]])
        push!(ranges, UnitRange{Int}(parse(Int, r[1]), parse(Int, r[2])))
    end

    types = Vector{DataType}()
    for l in data[2]
        t = strip(l)[1]
        if t == 'A'
            push!(types, String)
        elseif t == 'I'
            push!(types, Int)
        elseif t == 'F'
            push!(types, Float64)
        else
            error("Unsupported data type: " * t)
        end
    end

    units = Vector{String}()
    for l in data[3]; push!(units, strip(l)); end

    names = Vector{String}()
    for l in data[4]; push!(names, strip(l)); end

    comments = Vector{String}()
    for l in data[5]; push!(comments, strip(l)); end

    # Parse data
    data = parsefixedwidth(input, ranges)
    nrows = length(data[1])

    df = DataFrame()
    icol = Vector{Union{Missing,Int}}(missing, nrows)
    fcol = Vector{Union{Missing,Float64}}(missing, nrows)
    sym = Symbol.(names)
    for i in 1:length(ranges)
        s = data[i]
        if types[i] == String
            df[!,sym[i]] = s
        else
            j = findall(.!occursin.(Ref(r"^ *$"), s))
            if types[i] == Int
                if length(j) == nrows
                    df[!,sym[i]] = parse.(Int, s)
                else
                    icol .= missing
                    icol[j] .= parse.(Int, s[j])
                    df[!,sym[i]] = icol
                end
            elseif types[i] == Float64
                if length(j) == nrows
                    df[!,sym[i]] = parse.(Float64, s)
                else
                    fcol .= missing
                    fcol[j] .= parse.(Float64, s[j])
                    df[!,sym[i]] = fcol
                end
            end
        end
    end
    return df
end

function substMissing(df::DataFrame; mstring="NULL", mint=-999, mfloat=NaN)
    (nr, nc) = size(df)
    out = DataFrame()
    for icol in 1:nc
        tmp = df[icol]
        ig = .!ismissing.(tmp)
        im =   ismissing.(tmp)

        tt = Any
        for v in tmp
            if !ismissing(v)
                tt = typeof(v)
                break
            end
        end
        if tt == Any
            tt = (names(df))[icol]
            println("All values in column $tt are missing. Skipping column...")
            continue
        end

        col = Vector{tt}(undef, length(tmp))
        col[ig] .= tmp[ig]

        if tt <: AbstractString
            col[im] .= mstring
        elseif tt <: Integer
            col[im] .= mint
        elseif tt <: AbstractFloat
            col[im] .= mfloat
        else
            if length(im) > 0
                err = "Can't handle type: $tt"
                println("col=$icol")
                error(err)
            end
        end

        out[(names(df))[icol]] = col
    end
    return out
end

substMissing(v::Array{Float64,N}; mfloat=NaN) where N = v
function substMissing(v::Array{Union{Missing, Float64},N}; mfloat=NaN) where N
    o = deepcopy(v)
    i = findall(ismissing.(v))
    o[i] .= NaN
    return o
end


showrec(df::DataFrameRow) = 
    show(DataFrame(field=names(df), value=[values(df)...]), allrows=true, allcols=true)


end # module
