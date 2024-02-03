using AstroLib, SkyCoords, SortMerge, Healpix, Printf

export ra2string, dec2string, string2ra, string2dec, deg2dms, dms2deg, Jname2deg, pixelized_area, pixel_id, pixel_area, pixel_total, xmatch, best_match, sortmerge_cases


#=
Test the following with:

@assert deg2dms(dms2deg(1, 359, 59, 59.9999), round_decimals=2) == (1, 0, 0, 0.0)
@assert deg2dms(dms2deg(1, 359, 59, 59.9999), round_decimals=3) == (1, 0, 0, 0.0)
@assert deg2dms(dms2deg(1, 359, 59, 59.9999), round_decimals=4) == (1, 359, 59, 59.9999)
@assert deg2dms(dms2deg(-1, 359, 59, 59.9999), round_decimals=2) == (-1, 0, 0, 0.0)

cc = DB("SELECT RAd, DECd FROM All_info")
cc[!, :RAs]   = ra2string.( cc.RAd);
cc[!, :DECs]  = dec2string.(cc.DECd);
cc[!, :RAd2]  = string2ra.( cc.RAs);
cc[!, :DECd2] = string2dec.(cc.DECs);
extrema(cc.RAd .- cc.RAd2)     # (-2.0833333564951317e-6, 2.0833333564951317e-6)
extrema(cc.DECd .- cc.DECd2)   # (-1.3888888901192331e-6, 1.3888888901192331e-6)

dms2deg(1, 0, 0, 0.0005) * 15  # 2.083333333333333e-6
dms2deg(1, 0, 0, 0.005)        # 1.388888888888889e-6
=#


function deg2dms(deg::Float64; round_decimals=3)
    @assert round_decimals >= 1
    s = sixty(abs(deg))
    d = Int(s[1])
    m = Int(s[2])
    s = round(s[3] * 10^round_decimals) / 10^round_decimals
    if s >= 60
        newangle = dms2deg(1, d, m, s + 1 / 10^(round_decimals+1))
        newangle = mod(newangle, 360.)
        _, d, m, s = deg2dms(newangle, round_decimals=round_decimals)
    end
    @assert 0 <= d <= 359
    @assert 0 <= m <=  59
    @assert 0 <= s <   60
    return (Int(sign(deg)), d, m, s)
end

dms2deg(sign::Int, d::Int, m::Int, s::Float64) = sign * (d + m / 60. + s / 3600.)

ra2string(deg::Float64) = @sprintf("%02d:%02d:%06.3f", deg2dms(deg/15., round_decimals=3)[2:4]...)
string2ra(string::String) = dms2deg(1, Meta.parse.(split(strip(string), ':'))...) * 15.

function dec2string(deg::Float64)
    sign, d, m, s = deg2dms(deg, round_decimals=2)
    signsym = (sign ==- 1  ?  "-"  :  "+")
    return signsym * @sprintf("%02d:%02d:%05.2f", d, m, s)
end

function string2dec(string::String)
    d, m, s = Meta.parse.(split(strip(string), ':'))
    return dms2deg(strip(string)[1] == '-'  ?  -1  :  1, abs(d), m, s)
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


function sortmerge_cases(jj)
    # Not matched
    nmA = findall(countmatch(jj, 1) .== 0)
    nmB = findall(countmatch(jj, 2) .== 0)

    # Multiplicity of matched entries from both sides
    multiplicityA = countmatch(jj, 1)[jj[1]]
    multiplicityB = countmatch(jj, 2)[jj[2]]

    # Single matched
    sm  = SortMerge.subset(jj, findall((multiplicityA .== 1)  .&
                                       (multiplicityB .== 1)))

    # Multiple matched on one side, single match on the other
    mmA = SortMerge.subset(jj, findall((multiplicityA .>  1)  .&
                                       (multiplicityB .== 1)))
    mmB = SortMerge.subset(jj, findall((multiplicityA .== 1)  .&
                                       (multiplicityB .>  1)))

    # Multiple matched on both sides
    mm  = SortMerge.subset(jj, findall((multiplicityA .>  1)  .&
                                       (multiplicityB .>  1)))

    return (nmA=nmA, nmB=nmB, sm=sm, mmA=mmA, mmB=mmB, mm=mm)
end

#=
a = [1,2,2,3,4,6,6]
b = [6,6,2,3,3,4,5]
jj = sortmerge(a, b)
cc = sortmerge_cases(jj)
@assert a[cc.nmA] == [1]
@assert b[cc.nmB] == [5]
@assert a[cc.sm[1]] == [4]
@assert b[cc.sm[2]] == [4]
@assert a[cc.mmA[1]] == [3,3]
@assert b[cc.mmA[2]] == [3,3]
@assert a[cc.mmB[1]] == [2,2]
@assert b[cc.mmB[2]] == [2,2]
@assert a[cc.mm[1]] == [6,6,6,6]
@assert b[cc.mm[2]] == [6,6,6,6]
=#
