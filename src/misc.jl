using DataFrames

gaussian(x, mean=0., sigma=1.) =
    (1 / sqrt(2pi) / sigma) * exp(-((x - mean) / sigma)^2 / 2)

showv(df::DataFrameRow) =
    show(DataFrame(field=names(df), value=[values(df)...]), allrows=true, allcols=true)

function askpass(msg="")
    @assert Base.Sys.isunix()
    io = Base.getpass(msg)
    out = String(read(io))
    Base.shred!(io)
    return out
end
