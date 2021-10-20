using DataFrames, MySQL, DBInterface, ProgressMeter, DataStructures

export DBconnect, DBclose, DBtransaction, DBprepare, DB, @DB_str, DBsource, upload_table!


const DBConn = Vector{DBInterface.Connection}()
DBconnect() = DBConn[end]

function DBclose()
    DBInterface.close!.(DBConn)
    empty!(DBConn)
end

function DBconnect(host; user=nothing, pass=nothing, dbname=nothing)
    DBclose()

    if !isnothing(user)  &&  isnothing(pass)
        pass = askpass("Enter password for DB user $user")
    end
    conn = DBInterface.connect(MySQL.Connection, host, user, pass)

    # "Driver={MariaDB};SERVER=127.0.0.1"
    #conn = ODBC.Connection(host *
    #                       (isnothing(user)  ?  ""  :  ";USER=" * user * (
    #                           isnothing(pass)  ?  ""  :  ";PWD=" * pass)))
    push!(DBConn, conn)

    if !isnothing(dbname)
        DB("USE $dbname")
    end
    nothing
end


DBtransaction(f) = MySQL.transaction(f, DBconnect())
DBprepare(sql::AbstractString) = DBInterface.prepare(DBconnect(), string(sql))

DB(stmt, params...) = DBInterface.execute(stmt, params)
function DB(stmt, df::DataFrame)
    desc = split(stmt.sql)[1] * join(fill(" ", 9))
    desc = desc[1:9] * " "
    barlen = ProgressMeter.tty_width(desc, stderr, false)
    (barlen > 50)  &&  (barlen = 50)
    prog = Progress(nrow(df), desc=desc, dt=0.5, color=:light_black, barlen=barlen,
                    barglyphs=BarGlyphs('|','█', ['▏','▎','▍','▌','▋','▊','▉'],' ','|',))
    DBtransaction() do
        for (i, row) in enumerate(Tables.rows(df))
            ProgressMeter.update!(prog, i)
            DBInterface.execute(stmt, Tables.Row(row))
        end
    end
    nothing
end

function DB(sql::AbstractString)
    out = DataFrame(DBInterface.execute(DBconnect(), string(sql)))
    if  (ncol(out) == 0)  &&
        (nrow(out) == 0)
        return nothing
    end
    if  (ncol(out) == 1)  &&
        (nrow(out) == 1)
        return out[1,1]
    end
    return out
end

macro DB_str(sql)
    return :(DB($sql))
end

function DBsource(file::AbstractString, subst::Vararg{Pair{String,String}, N}) where N
    delim = ";"
    sql = ""
    for line in readlines(file)
        if !isnothing(match(r"delimiter"i, line))
            if line == "DELIMITER ;"
                delim = ";"
            elseif line == "DELIMITER //"
                delim = "//"
            else
                error("Unexpected delimiter statement: $line")
            end
        else
            if !isnothing(match(Regex(delim), line))
                s = string.(strip.(split(line, delim)))
                @assert length(s) == 2 "Too many delimiters in one line: $line"
                sql *= s[1]
                for r in subst
                    sql = replace(sql, r)
                end
                println(sql)
                DB(sql)
                sql = ""
                (s[2] != "")  &&  (sql = s[2] * "\n")
            else
                sql *= line * "\n"
            end
        end
    end
    if strip(sql) != ""
        for r in subst
            sql = replace(sql, r)
        end
        println(sql)
        DB(sql)
    end
end



mutable struct DBColumn{T}
    sql::String
    null::Bool
    length::Int

    function DBColumn(data::AbstractVector{T}, sql) where T
        new{T}(sql, false, 0)
    end

    function DBColumn(data::AbstractVector{Union{Missing, T}}, sql) where T
        @assert !isa(T, Union)
        new{T}(sql, true, 0)
    end

    function DBColumn(data::AbstractVector{Vector{T}}, sql) where T
        @assert !isa(T, Union)
        new{T}(sql, false, length(data[1]))
    end
end

DBColumn(data::Vector{Union{Missing, Float32}}) = DBColumn(data, "FLOAT")
function DBColumn(data::Vector{Float32})
    if count(isnan.(data)) > 0
        data = convert(Vector{Union{Missing, Float32}}, data)
    end
    return DBColumn(data, "FLOAT")
end

DBColumn(data::Vector{Union{Missing, Float64}}) = DBColumn(data, "DOUBLE")
function DBColumn(data::Vector{Float64})
    if count(isnan.(data)) > 0
        data = convert(Vector{Union{Missing, Float64}}, data)
    end
    return DBColumn(data, "DOUBLE")
end

DBColumn(data::Vector{Union{Missing, UInt8 }}) = DBColumn(data, "TINYINT  UNSIGNED")
DBColumn(data::Vector{               UInt8  }) = DBColumn(data, "TINYINT  UNSIGNED")
DBColumn(data::Vector{Union{Missing,  Int8 }}) = DBColumn(data, "TINYINT    SIGNED")
DBColumn(data::Vector{                Int8  }) = DBColumn(data, "TINYINT    SIGNED")
DBColumn(data::Vector{Union{Missing, UInt16}}) = DBColumn(data, "SMALLINT UNSIGNED")
DBColumn(data::Vector{               UInt16 }) = DBColumn(data, "SMALLINT UNSIGNED")
DBColumn(data::Vector{Union{Missing,  Int16}}) = DBColumn(data, "SMALLINT   SIGNED")
DBColumn(data::Vector{                Int16 }) = DBColumn(data, "SMALLINT   SIGNED")
DBColumn(data::Vector{Union{Missing, UInt32}}) = DBColumn(data, "     INT UNSIGNED")
DBColumn(data::Vector{               UInt32 }) = DBColumn(data, "     INT UNSIGNED")
DBColumn(data::Vector{Union{Missing,  Int32}}) = DBColumn(data, "     INT   SIGNED")
DBColumn(data::Vector{                Int32 }) = DBColumn(data, "     INT   SIGNED")
DBColumn(data::Vector{Union{Missing, UInt64}}) = DBColumn(data, "  BIGINT UNSIGNED")
DBColumn(data::Vector{               UInt64 }) = DBColumn(data, "  BIGINT UNSIGNED")
DBColumn(data::Vector{Union{Missing,  Int64}}) = DBColumn(data, "  BIGINT   SIGNED")
DBColumn(data::Vector{                Int64 }) = DBColumn(data, "  BIGINT   SIGNED")
DBColumn(data::Vector{Union{Missing,   Bool}}) = DBColumn(data, "TINYINT    SIGNED")
DBColumn(data::Vector{                 Bool }) = DBColumn(data, "TINYINT    SIGNED")

function DBColumn(data::Vector{Union{Missing, Symbol}})
    i = findall(.!ismissing.(data))
    @assert length(i) > 0 "All values are missing"
    return DBColumn(data, "ENUM(" * join("'" .* sort(unique(string.(data[i]))) .* "'", ", ") * ")")
end

DBColumn(data::Vector{Symbol}) =
    return DBColumn(data, "ENUM(" * join("'" .* sort(unique(string.(data))) .* "'", ", ") * ")")


function DBColumn(data::Vector{Union{Missing, String}})
    i = findall(.!ismissing.(data))
    if length(i) == 0
        @warn "All values are missing, assuming a length of 20"
        maxlen = 20
    else
        maxlen = maximum(length.(data[i]))
    end
    return DBColumn(data, "VARCHAR($(maxlen))")
end

function DBColumn(data::Vector{String})
    maxlen = maximum(length.(data))
    return DBColumn(data, "VARCHAR($(maxlen))")
end


prepare_column!(data::DataFrame, col::DBColumn, name::Symbol) =
    col.null  &&  allowmissing!(data, name)


function prepare_column!(data::DataFrame, col::Union{DBColumn{Float32}, DBColumn{Float64}}, name::Symbol)
    i = findall(.!ismissing.(data[:, name])  .&  isnan.(data[:, name]))
    @assert (length(i) == 0)  ||  col.null
    if col.null
        allowmissing!(data, name)
        data[i, name] .= missing
    end
end

function prepare_column!(data::DataFrame, col::DBColumn{Bool}, name::Symbol)
    data[!, name] = fill(Int8(0), nrow(data))
    col.null  &&  allowmissing!(data, name)
    i = findall(  ismissing.(data[:, name]));  data[i, name] .= missing
    i = findall(.!ismissing.(data[:, name]));  data[i, name] .= Int8(data[:, name])
end



function DBColumns(data::DataFrame)
    out = OrderedDict{Symbol, DBColumn}()
    for name in Symbol.(names(data))
        out[name] = DBColumn(data[:, name])
    end
    return out
end

upload_table!(data::DataFrame, tbl_name::String; kw...) =
    upload_table!(data, DBColumns(data), tbl_name; kw...)

function upload_table!(data::DataFrame, meta::OrderedDict{Symbol, DBColumn}, tbl_name::String; drop=true, temp=false, memory=false)
    coldefs = Vector{String}()
    for name in Symbol.(names(data))
        @info "Preparing column $name"
        col = meta[name]
        if col.length == 0
            prepare_column!(data, col, name)
            push!(coldefs, "`$(name)` $(col.sql) " * (col.null  ?  ""  :  " NOT NULL"))
        else
            for i in 1:col.length
                data[!, Symbol(name, i)] .= getindex.(data[:, name], i)
                prepare_column!(data, col, Symbol(name, i))
                push!(coldefs, "`$(name)$(i)` $(col.sql) " * (col.null  ?  ""  :  " NOT NULL"))
            end
            select!(data, Not(name))
        end
    end

    if drop
        DB("DROP TABLE IF EXISTS $tbl_name")
        sql = "CREATE " * (temp ? "TEMPORARY" : "") * " TABLE $tbl_name"
        sql *= " ( " * join(coldefs, ", ") * ")"
        memory  &&  (sql *= " ENGINE=MEMORY")
        println(sql)
        DB(sql)
    end
    params = join(repeat("?", ncol(data)), ",")
    stmt = DBprepare("INSERT INTO $tbl_name VALUES ($params)")
    DB(stmt, data)
    nothing
end
