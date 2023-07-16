using AstroLib, SkyCoords, SortMerge, Healpix, Printf

export ra2string, dec2string, string2ra, string2dec, hms2ra, dms2dec, Jname2deg, pixelized_area, pixel_id, pixel_area, pixel_total, xmatch, best_match

ra2string(d::Float64)  = @sprintf(" %02d:%02d:%05.2f", sixty(d/15.)...)
dec2string(d::Float64) = (d < 0  ?  "-"  :  "+") * @sprintf("%02d:%02d:%05.2f", sixty(abs(d))...)

function string2ra(c::String)
    s = Meta.parse.(split(strip(c), ':'))
    @assert length(s) == 3
    return hms2ra(s...)
end

function string2dec(c::String)
    sign = "+"
    first = strip(c)[1]
    (first == '-')  &&  (sign = "-")
    s = Meta.parse.(split(strip(c), ':'))
    @assert length(s) == 3
    return dms2dec(sign, abs(s[1]), s[2], s[3])
end

hms2ra(h, m, s) = (h + m / 60. + s / 3600.) * 15.
function dms2dec(S, d, m, s)
    @assert S in ["+", "-"]
    sign = (S == "+"  ?  1.  :  -1.)
    return sign * (d + m / 60. + s / 3600.)
end


function Jname2deg(name)
    @assert name[1] == 'J'
    i = findfirst("+", name)
    if isnothing(i)
        i = findfirst("-", name)
    end
    i = i[1]
    RAs = name[2:i-1]
    DECs = name[i:end]
    @assert RAs[ 7] == '.'
    @assert DECs[8] == '.'
    return ten(RAs[1:2] * ":" * RAs[3:4] * ":" * RAs[5:end]) * 15,
    ten(DECs[1:3] * ":" * DECs[4:5] * ":" * DECs[6:end])
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

pixel_total(order) = nside2npix(2^order)

pixel_area(order) = nside2pixarea(2^order) * ((180/pi)^2)

function pixel_id(order, RAd, DECd)
    @assert 0 < order < 13
    nside = 2^order
    rad = 180/pi
    ang2pixNest(Healpix.Resolution(nside), (90 .- DECd) ./ rad, RAd ./ rad)
end


function best_match(ra1::Vector{T1}, de1::Vector{T1},
                    ra2::Vector{T2}, de2::Vector{T2},
                    jj::SortMerge.Matched;
                    side=:both, invert=false) where
    {T1 <: AbstractFloat, T2 <: AbstractFloat}

    if  (maximum(countmatch(jj, 1)) == 1)  &&
        (maximum(countmatch(jj, 2)) == 1)
        return jj  # nothing to do
    end

    out = Vector{Vector{Bool}}()
    for side in 1:2
        isort = sortperm(jj[side])
        jj1 = jj[1][isort]
        jj2 = jj[2][isort]
        dist = gcirc.(2, ra1[jj1], de1[jj1], ra2[jj2], de2[jj2])
        best = fill(true, length(dist))

        k1 = 1
        while k1 < length(dist)
            k2 = k1
            ii = (side == 1  ?  jj1  :  jj2)
            while ii[k1] == ii[k2]
                k2 += 1
                (k2 > length(dist))  &&  break
            end
            k2 -= 1
            if k2 > k1
                best[k1:k2] .= false
                kk = k1 - 1 + argmin(dist[k1:k2])
                best[kk] = true
            end
            k1 = k2 + 1
        end
        isort = sortperm(isort)
        best = best[isort]
        push!(out, best)
    end

    best1 = out[1]
    best2 = out[2]
    if side == 1
        selected = findall(xor.(best1, invert))
    elseif side == 2
        selected = findall(xor.(best2, invert))
    elseif side == :both
        selected = findall(xor.(best1 .& best2, invert))
    else
        error("Unrecognized value for best keyword: $best")
    end
    return SortMerge.subset(jj, selected)
end


function xmatch(tabA::DataFrame, coordsA::NTuple{2, Symbol},
                tabB::DataFrame, coordsB::NTuple{2, Symbol},
                thresh_arcsec::Real)
    @assert issorted(tabA[:, coordsA[2]])
    @assert issorted(tabB[:, coordsB[2]])

    ra1 = tabA[:, coordsA[1]]
    de1 = tabA[:, coordsA[2]]
    ra2 = tabB[:, coordsB[1]]
    de2 = tabB[:, coordsB[2]]
    thresh_deg = thresh_arcsec / 3600. # [deg]

    function sd(c1, c2, i1, i2)
        dd = de1[i1] - de2[i2]
        (dd < -thresh_deg)  &&  (return -1)
        (dd >  thresh_deg)  &&  (return  1)
        dd = gcirc(2,
                   ra1[i1], de1[i1],
                   ra2[i2], de2[i2])
        (dd <= thresh_arcsec)  &&  (return 0)
        return 999
    end
    out = sortmerge(1:nrow(tabA), 1:nrow(tabB),
                    sd=sd, sorted=true)
    return out
end


function xmatch(ra1::Vector{T1}, de1::Vector{T1},
                ra2::Vector{T2}, de2::Vector{T2},
                thresh_arcsec::Real; sorted=false) where
    {T1 <: AbstractFloat, T2 <: AbstractFloat}

    lt(v, i, j) = ((v[i, 2] - v[j, 2]) < 0)
    function sd(c1, c2, i1, i2, thresh_arcsec)
        thresh_deg = thresh_arcsec / 3600. # [deg]
        dd = c1[i1, 2] - c2[i2, 2]
        (dd < -thresh_deg)  &&  (return -1)
        (dd >  thresh_deg)  &&  (return  1)
        dd = gcirc(2, c1[i1, 1], c1[i1, 2], c2[i2, 1], c2[i2, 2])
        (dd <= thresh_arcsec)  &&  (return 0)
        return 999
    end
    @assert all(isfinite.(ra1))
    @assert all(isfinite.(de1))
    @assert all(isfinite.(ra2))
    @assert all(isfinite.(de2))
    out = sortmerge([ra1 de1], [ra2 de2], thresh_arcsec,
                    lt1=lt, lt2=lt,
                    sd=sd, sorted=sorted)
    return out
end


function xmatch(ra1::Vector{T1}, de1::Vector{T1}, thresh_arcsec1::Vector{<:Real},
                ra2::Vector{T2}, de2::Vector{T2}, thresh_arcsec2::Vector{<:Real};
                sorted=false) where
    {T1 <: AbstractFloat, T2 <: AbstractFloat}

    lt(v, i, j) = ((v[i, 2] - v[j, 2]) < 0)
    function sd(c1, c2, i1, i2, thresh_arcsec1, thresh_arcsec2, thresh_deg)
        dd = c1[i1, 2] - c2[i2, 2]
        (dd < -thresh_deg)  &&  (return -1)
        (dd >  thresh_deg)  &&  (return  1)
        dd = gcirc(2, c1[i1, 1], c1[i1, 2], c2[i2, 1], c2[i2, 2])
        thresh_arcsec = max(thresh_arcsec1[i1], thresh_arcsec2[i2])
        (dd <= thresh_arcsec)  &&  (return 0)
        return 999
    end
    @assert all(isfinite.(ra1))
    @assert all(isfinite.(de1))
    @assert all(isfinite.(ra2))
    @assert all(isfinite.(de2))

    max_thresh = 0.
    try
        # Ignore errors when one or both arrays are empty
        max_thresh = max(maximum(thresh_arcsec1), maximum(thresh_arcsec2))
    catch
    end
    # If there's only a single threshold value use the simpler (and
    # faster) algorithm
    if  all(max_thresh .== thresh_arcsec1)  &&
        all(max_thresh .== thresh_arcsec2)
        return xmatch(ra1, de1, ra2, de2, max_thresh)
    end

    max_thresh_deg = max_thresh / 3600.
    out = sortmerge([ra1 de1], [ra2 de2], thresh_arcsec1, thresh_arcsec2, max_thresh_deg,
                    lt1=lt, lt2=lt,
                    sd=sd, sorted=sorted)
    return out
end
