module AstroRecipes

using DataFrames, FITSIO, AstroLib, SkyCoords, StructC14N, SortMerge, WCS, ODBC, Printf, Healpix

import DataFrames.DataFrame, Base.write, DBInterface.execute

export gaussian, showv, ra2string, fits2df, dec2string, xmatch, df2dbtable, parsecatalog, pixelized_area

gaussian(x, mean=0., sigma=1.) =
    (1 / sqrt(2pi) / sigma) * exp(-((x - mean) / sigma)^2 / 2)

showv(df::DataFrameRow) =
    show(DataFrame(field=names(df), value=[values(df)...]), allrows=true, allcols=true)


ra2string(d::Float64)  = @sprintf(" %02d:%02d:%05.2f", sixty(d/15.)...)
dec2string(d::Float64) = (d < 0  ?  "-"  :  "+") * @sprintf("%02d:%02d:%05.2f", sixty(abs(d))...)


DataFrame(::Type{DataFrame}, hdu::FITSIO.ImageHDU) = error("Can not convert an ImageHDU extension to a DataFrame")
function DataFrame(hdu::FITSIO.HDU)
    fld = join.(split.(FITSIO.colnames(hdu), '.'), '_')
    out = DataFrame()
    for i in 1:length(fld)
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
        else
            push!(data, dfr[:,name])
        end
    end
    write(f, string.(names(dfr)), data)
end


function xmatch(ra1::Vector{T1}, de1::Vector{T1},
                ra2::Vector{T2}, de2::Vector{T2},
                thresh_asec::T3; sorted=false, quiet=false) where
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
    @assert all(isfinite.(ra1))
    @assert all(isfinite.(de1))
    @assert all(isfinite.(ra2))
    @assert all(isfinite.(de2))
    return sortmerge([ra1 de1], [ra2 de2], thresh_asec, lt1=lt, lt2=lt, sd=sd, sorted=sorted, quiet=quiet)
end


function df2dbtable(_df::DataFrame)
    df = deepcopy(_df)
    colnames = Vector{Symbol}()
    for name in names(df)
        if isa(df[1, name], AbstractVector)
            for i in 1:length(df[1, name])
                df[!, Symbol(name, i)] .= getindex.(df[:, name], i)
                push!(colnames, Symbol(name, i))
            end
        else
            push!(colnames, Symbol(name))
        end
    end
    select!(df, colnames)

    dbtype = Vector{String}()
    for i in 1:ncol(df)
        tn = eltype(df[[], i])
        if isa(tn, Union)
            t = nonmissingtype(tn)
            @assert isa(t, DataType) "Type not supported: $tn"
            hasnull = true
        else
            t = tn
            hasnull = false
        end

        notnull = (hasnull  ?  ""  :  "NOT NULL")
        if t == Float32
            hasnull  ||   allowmissing!(df, i)
            df[(.!ismissing.(df[:, i]))  .&  (.!isfinite.(df[:, i])), i] .= missing
            push!(dbtype, "FLOAT")
        elseif t == Float64
            hasnull  ||   allowmissing!(df, i)
            df[(.!ismissing.(df[:, i]))  .&  (.!isfinite.(df[:, i])), i] .= missing
            push!(dbtype, "DOUBLE")
        elseif t == Int8
            push!(dbtype, "TINYINT SIGNED $notnull")
        elseif t == Int16
            push!(dbtype, "SMALLINT SIGNED $notnull")
        elseif t == Int32
            push!(dbtype, "INT SIGNED $notnull")
        elseif t == Int64
            push!(dbtype, "BIGINT SIGNED $notnull")
        elseif t == Bool
            push!(dbtype, "BOOLEAN $notnull")
        elseif t == Symbol
            df[!, i] .= string(df[:, i])
            push!(dbtype, "ENUM(" * join("'" .* sort(unique(string.(df[:, i]))) .* "'", ", ") * ") $notnull")
        elseif t == String
            push!(dbtype, "VARCHAR(" * string(maximum(length.(df[:, i]))) * ") $notnull")
        else
            error("Type not supported: $t")
        end
    end

    sql = "`" .* string.(names(df)) .* "` " .* dbtype
    return (sql, df)
end


function df2dbtable(conn::ODBC.Connection, _df::DataFrame, name; drop=true)
    (sql, df) = df2dbtable(_df)

    if drop
        DBInterface.execute(conn, "DROP TABLE IF EXISTS $name")
        sql = "CREATE TABLE IF NOT EXISTS $name (" * join(sql, ", ") * ")"
        #@info sql
        DBInterface.execute(conn, sql)
    end

    params = join(repeat("?", ncol(df)), ",")
    stmt = DBInterface.prepare(conn, "INSERT INTO $name VALUES ($params)")
    DBInterface.execute(conn, stmt, df)
    end
end


function DBInterface.execute(conn::ODBC.Connection, stmt::ODBC.Statement, df::DataFrame)
     ODBC.transaction(conn) do
        DBInterface.execute(stmt, df)
     end
end


function DBInterface.execute(stmt::ODBC.Statement, df::DataFrame)
    N = nrow(df)
    T0 = Base.time_ns()
    for (i, row) in enumerate(Tables.rows(df))
        if mod(i, 100) == 0
            @printf("Done: %6.1f%%   (%.4g records/sec)\r", 100 * i / N, 1e9 * i / (Base.time_ns() - T0))
        end
        DBInterface.execute(stmt, Tables.Row(row))
    end
    println()
end


function pixelized_area(RAd, DECd)
    rad = 180/pi
    @printf("%6s  %12s  %12s  %12s\n", "Order", "Npix", "Area [deg^2]", "% diff")
    last = NaN
    for order in 0:13
        nside = 2^order
        res = Healpix.Resolution(nside)
        n = length(unique(ang2pixNest.(Ref(res), (90 .- DECd) ./ rad, RAd ./ rad)))
        area = n * nside2pixarea(nside) * rad^2
        pdiff = 100 * (last - area) / ((last + area) / 2)
        last = area
        @printf("%6d  %12d  %12.4f  %12.4f\n", order, n, area, pdiff)
    end
end

end # module
