using DataFrames, Unitful, UnitfulAstro, Statistics, CSV, StatsBase

export compare_df, strip_blanks!, gpc, showv, splitrange


gaussian(x, mean=0., sigma=1.) =
    (1 / sqrt(2pi) / sigma) * exp(-((x - mean) / sigma)^2 / 2)

function showv(df::DataFrame)
    out = DataFrame(field=names(df))
    for i in 1:nrow(df)
        out[!, Symbol("Row", i)] .= [values(df[i, :])...]
    end
    show(out, allrows=true, allcols=true)
end

showv(df::DataFrameRow) = showv(DataFrame(df))


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
        if nonmissingtype(eltype(df[:, i])) == String
            df[:, i] .= string.(strip.(df[:, i]))
        end
    end
end


# Reference: http://www.astro.wisc.edu/~dolan/constants.html
Base.@kwdef struct gpc
    # Dimensionless or common to both CGS and MKS
    jd_mjd    = 2400000.5              # Offset to change from Julian date to MJD
    NA        = 6.0221367    * 1e23    # Avagadro's number
    α         = 7.29735308   * 1e-3    # Fine structure constant
    day       = 86400.                 # days to sec fator
    month     = 2592000.               # months to sec fator
    year      = 31536000.              # year to sec fator

    # CGS
    c         = 2.99792458  * 1e10     * u"cm" / u"s"                          # Vacuum speed of light
    h         = 6.6260755   * 1e-27    * u"erg" * u"s"                         # Planck's constant
    ħ         = 1.05457266  * 1e-27    * u"erg" * u"s"                         # Reduced Planck's constant
    G         = 6.67259     * 1e-8     * u"cm"^3 * u"g"^-1 * u"s"^-2           # Gravitational constant
    e         = 4.8032068   * 1e-10                                            # Electron charge (esu)
    me        = 9.1093897   * 1e-28    * u"g"                                  # Mass of electron
    mp        = 1.6726231   * 1e-24    * u"g"                                  # Mass of proton
    mn        = 1.6749286   * 1e-24    * u"g"                                  # Mass of neutron
    mH        = 1.6733      * 1e-24    * u"g"                                  # Mass of hydrogen
    amu       = 1.6605402   * 1e-24    * u"g"                                  # Atomic mass unit
    k         = 1.380658    * 1e-16    * u"erg" / u"K"                         # Boltzmann constant
    eV        = 1.6021772   * 1e-12    * u"erg" / u"eV"                        # Electron volt to erg factor
    a         = 7.5646      * 1e-15    * u"erg" * u"cm"^-3 * u"K"^-4           # Radiation density constant
    σ         = 5.67051     * 1e-5     * u"erg" * u"cm"^-2 * u"K"^-4 * u"s"^-1 # Stefan-Boltzmann constant
    R_inf     = 1.097373    * 1e5      * u"cm"^-1                              # R_infinity
    au        = 1.496       * 1e13     * u"cm" / u"AU"                         # Astronomical unit to cm
    pc        = 3.086       * 1e18     * u"cm" / u"pc"                         # Parsec to cm factor
    ly        = 9.463       * 1e17     * u"cm" / u"ly"                         # Light year to cm factor
    ld        = 2.5902      * 1e15                                             # Light day to cm factor (cm ly^-1)
    Msun      = 1.99        * 1e33     * u"g"                                  # Solar mass
    Rsun      = 6.96        * 1e10     * u"cm"                                 # Solar radius
    Lsun      = 3.9         * 1e33     * u"erg" / u"s"                         # Solar luminosity
    Tsun      = 5.780       * 1e3      * u"K"                                  # Solar temperature
    Mearth    = 5.974       * 1e27     * u"g"                                  # Earth mass
    Rearth    = 6372.8      * 1e5      * u"cm"                                 # Earth mean radius
    g         = 981.52                 * u"cm" * u"s"^-2                       # Acceleration at Earth surface
    thom      = 0.66524616  * 1e-24    * u"cm"^2                               # Thomson cross section
    jansky    = 1e-23                  * u"erg" * u"cm"^-2 * u"s"^-1 * u"Hz"^-1# Flux density (keV cm^-2 s^-1 keV^/1)
    wien      = 2.82 * k / h
    deg       = pi/180                 * u"rad" * u"°"^-1
    arcsec    = pi/180/3600                                                    # rad arcsec^-1
    mas       = pi/180/3600/1000                                               # rad milliarcsec^-1
    edd       = 1.26e38                * u"erg" * u"s"^-1 * u"Msun"^-1         # Eddington luminosity (erg s^-1 M_sun^-1)
    r_cm      = 2 * pi^2 * me * e^4 / (h^3) / c                                # Rydberg constant (cm^-1)
    r2_cm     = r_cm / (1 + me/mp)                                             # Rydberg constant (cm^-1, reduced mass)
    A         = 1.e-8                  * u"cm" / Unitful.angstrom
end


function smooth(y, n)
    out = y .* 1.
    @assert mod(n, 2) == 1
    @assert n >= 3
    h = div(n-1, 2)
    for i in 1+h:length(y)-h
        out[i] = mean(y[i-h:i+h])
    end
    return out
end


function rebin(y, n)
    out = Float64[]
    @assert mod(n, 2) == 1
    @assert n >= 3
    h = div(n-1, 2)
    for i in 1+h:n:length(y)-h
        push!(out, mean(y[i-h:i+h]))
    end
    return out
end


function ppvalunc(v, e)
    f = 10^ceil(-log10(e))
    pe = round(e * f) / f
    if isinteger(log10(pe))  &&  pe <= e
        # if the only sig. digit is 1
        f *= 10
        pe = round(e * f) / f
    end
    pv = round(v * f) / f
    # println(v, " ", e, " ", f, " ", pv, " ", pe)
    return pv, pe
end


function splitrange(total_size, chunk_size)
    out = Vector{NTuple{3, Int}}()
    for ichunk in 1:Int(ceil(total_size / chunk_size))
        i0 = (ichunk-1) * chunk_size + 1
        i1 =         i0 + chunk_size - 1
        (i1 > total_size)  &&  (i1 = total_size)
        push!(out, (ichunk, i0, i1))
    end
    return out
end


function csv2df(args...; delim=',', header=nothing, stringtype=String, kws...)
    # Invoke CSV.File
    if isa(header, Vector{Symbol})
        df = CSV.File(args...; header=header, delim=delim, stringtype=stringtype, kws...) |> DataFrame
    else
        df = CSV.File(args...;                delim=delim, stringtype=stringtype, kws...) |> DataFrame
    end

    for i in 1:ncol(df)
        # Replace columns containing just missing values with empty strings
        if eltype(df[:, i]) == Missing
            df[!, i] .= ""
        end
    end

    # Join all columns past the last given one into a single one
    if isa(header, Vector{Symbol})    &&
        (ncol(df) > length(header))   &&
        (nonmissingtype(eltype(df[:, length(header)])) == String)
        for i in 1:nrow(df)
            t = Tuple(df[i, length(header):ncol(df)])
            all(ismissing.(t))  &&  continue
            df[i, length(header)] = join(string.(skipmissing(t)), delim)
        end
        select!(df, 1:length(header))
    end
    return df
end



function countmapdf(vv)
    cm = countmap(vv)
    out = DataFrame(:value => collect(keys(cm)), :count => collect(values(cm)))
    sort!(out, :count)
    return out
end
