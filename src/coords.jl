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
