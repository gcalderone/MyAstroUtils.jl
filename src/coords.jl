using AstroLib, SkyCoords, SortMerge, Healpix, Printf

export ra2string, dec2string, pixelized_area, xmatch

ra2string(d::Float64)  = @sprintf(" %02d:%02d:%05.2f", sixty(d/15.)...)
dec2string(d::Float64) = (d < 0  ?  "-"  :  "+") * @sprintf("%02d:%02d:%05.2f", sixty(abs(d))...)

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


function xmatch_best(ra1::Vector{T1}, de1::Vector{T1},
                     ra2::Vector{T2}, de2::Vector{T2},
                     jj::SortMerge.Matched) where
    {T1 <: AbstractFloat, T2 <: AbstractFloat}
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
    return out[1], out[2]
end


function xmatch(ra1::Vector{T1}, de1::Vector{T1},
                ra2::Vector{T2}, de2::Vector{T2},
                thresh_arcsec::Real; sorted=false, quiet=false, best=nothing, invert=false) where
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
                    lt1=lt, lt2=lt, sd=sd, sorted=sorted, quiet=quiet)

    if !isnothing(best)  &&
        ((maximum(countmatch(out, 1)) > 1)  ||
         (maximum(countmatch(out, 2)) > 1))
        (best1, best2) = xmatch_best(ra1, de1, ra2, de2, out)
        if best == 1
            selected = findall(xor.(best1, invert))
        elseif best == 2
            selected = findall(xor.(best2, invert))
        elseif best == :both
            selected = findall(xor.(best1 .& best2, invert))
        else
            error("Unrecognized value for best keyword: $best")
        end
        quiet  ||  println("Dropping $(nmatch(out) - length(selected)) matching pairs")
        out = SortMerge.subset(out, selected)
    end

    return out
end
