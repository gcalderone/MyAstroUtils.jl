# MyAstroUtils.SID_conesearch("Catalogs2.LegacySurveySweep_10p1", [63.8104, 69.2504], [-40.395, -39.77], [100., 100])

SID_conesearch(table::String, RAd::Float64, DECd::Float64, radius_arcsec::Float64; kws...) = SID_conesearch(table, [0], [RAd], [DECd], [radius_arcsec]; kws...)
function SID_conesearch(table::String, id::Vector{Int}, RAd::Vector{T}, DECd::Vector{T}, radius_arcsec::Vector{<: Real}) where T <: AbstractFloat
    @assert length(id) > 0
    @assert length(id) == length(RAd) == length(DECd) == length(radius_arcsec)
    DB("CALL SID.InitSearch('$(table)')")
    for i in 1:length(RAd)
        DB("CALL SID.AddCone($(id[i]), $(RAd[i]), $(DECd[i]), $(radius_arcsec[i]) / 3600.)")
    end
    sql = DB("SELECT SID.get_query()")
    DB(sql)
end
