using ProgressMeter

function fallocate_file(filepath::String, size_bytes::Integer)
    file = try
        open(filepath, "w+")
    catch e
        error("Failed to open file '$filepath': $e")
    end

    fd = Base.fd(file)


    mode = 0
    offset = 0
    len = size_bytes

    result = ccall((:fallocate, "/lib/x86_64-linux-gnu/libc.so.6"), Cint,
        (Cint, Cint, Clonglong, Clonglong),
        fd, mode, offset, len
    )

    close(file)

    if result != 0
        errno_code = Libc.errno()
        error("fallocate failed for '$filepath': $(Libc.strerror(errno_code)) (Error Code $errno_code)")
    end
    return true
end



string2VecUInt8(s::String) = UInt8.([c for c in s])
find_in_file(infile::String, needle::String       ; kws...) = find_in_file(infile, [needle]; kws...)
find_in_file(infile::String, needle::Vector{UInt8}; kws...) = find_in_file(infile, [needle]; kws...)
find_in_file(infile::String, needles::Vector{String}; kws...) = find_in_file(infile, [string2VecUInt8(needle) for needle in needles]; kws...)

function find_in_file(infile::String, needles::Vector{Vector{UInt8}};
                      OVERLAP::Int=maximum(length.(needles)),
                      CHUNK_SIZE::Int=50 * 1024 * 1024)
    @assert all(length.(needles) .>= 1)
    @assert OVERLAP >= maximum(length.(needles))
    @assert CHUNK_SIZE > OVERLAP
    output = Vector{Tuple{Int64, UnitRange{Int64}}}()

    buffer = Vector{UInt8}(undef,           OVERLAP + CHUNK_SIZE)
    buffer_view = view(buffer, (OVERLAP+1):(OVERLAP + CHUNK_SIZE))
    @assert length(buffer_view) == CHUNK_SIZE

    input = open(infile , "r")
    total_read = 0
    prog = Progress(stat(infile).size, dt=0.5, showspeed=true)
    while !eof(input)
        ProgressMeter.update!(prog, total_read)
        curlen = readbytes!(input, buffer_view)

        for ineedle in 1:length(needles)
            needle = needles[ineedle]
            for i in findall(buffer .== needle[1])
                if (total_read == 0)  &&  (i <= OVERLAP)
                    continue # I may be found something among the "undef" which is not relevant
                end
                j = i:(i+length(needle)-1)
                if length(buffer) >= j[end]
                    if all(buffer[j] .== needle)
                        k = j .+ (total_read - OVERLAP)
                        new = (ineedle, k)
                        # Same stem may be found in both the "overlap"
                        # region and at the end of the buffer.  Here
                        # I'll add only if it is a new finding.
                        if (length(output) == 0)  ||   (output[end] != new)
                            push!(output, new)
                        end
                        println("\ndd if=$(infile) bs=1 count=$(OVERLAP) status=none skip=$(k[1]-1) | hexdump -C")
                    end
                end
            end
        end

        total_read += curlen
        buffer[1:OVERLAP] .= buffer[(end-OVERLAP+1):end]
    end
    close(input)
    return output
end



replace_in_file(infile::String,
                oldnew::Vector{Pair{String, String}};
                kws...) = replace_in_file(infile, [string2VecUInt8(old) => string2VecUInt8(new) for (old, new) in oldnew]; kws...)

replace_in_file(infile::String,
                oldnew::Pair{String, String};
                kws...) = replace_in_file(infile, [string2VecUInt8(oldnew[1]) => string2VecUInt8(oldnew[2])]; kws...)


function replace_in_file(infile::String,
                         oldnew::Vector{Pair{Vector{UInt8}, Vector{UInt8}}};
                         inplace=false,
                         CHUNK_SIZE::Int=50 * 1024 * 1024,
                         kws...)
    replace_indices = find_in_file(infile, getindex.(oldnew, 1); CHUNK_SIZE=CHUNK_SIZE, kws...)
    (length(replace_indices) == 0)  &&  (return nothing)

    if inplace
        for (old, new) in oldnew
            @assert length(old) == length(new) "Can't modify in place ($(String(old)), $(String(new)))"
        end

        infile_size = stat(infile).size
        f = open(infile, "r+")
        for (ineedle, indices) in replace_indices
            old, new = oldnew[ineedle]
            seek(f, indices[1] - 1)
            write(f, new)
        end
        truncate(f, infile_size) # TODO: why is this needed?
        close(f)
    else
        outfile = ""
        for i in 1:typemax(Int64)
            outfile = infile * ".modified.$(i)"
            if !isfile(outfile)
                break
            end
        end
        println("Writing into $outfile ...")
        input  = open( infile, "r")
        output = open(outfile, "w")
        buffer = Vector{UInt8}(undef, CHUNK_SIZE)
        total_read = 0

        ineedle, indices = popfirst!(replace_indices)
        old, new = oldnew[ineedle]

        while !eof(input)
            do_replace = false
            if (ineedle == 0)  ||  (total_read + CHUNK_SIZE < indices[1] - 1)
                bytes_to_read = CHUNK_SIZE
            else
                bytes_to_read = indices[1] - 1 - total_read
                do_replace = true
            end

            if bytes_to_read > 0
                vv = view(buffer, 1:bytes_to_read)

                curlen = readbytes!(input, vv)
                if curlen == length(vv)
                    @assert write(output, vv) == length(vv)
                else
                    @assert curlen < length(vv)
                    @assert write(output, vv[1:curlen]) == curlen
                end
                total_read += curlen
            end

            if do_replace
                @assert write(output, new) == length(new)
                seek(input, indices[end])
                total_read += length(indices)

                if length(replace_indices) > 0
                    ineedle, indices = popfirst!(replace_indices)
                    old, new = oldnew[ineedle]
                else
                    ineedle = 0
                end
            end
        end
        close(input)
        close(output)
    end

    return nothing
end
