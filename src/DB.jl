using DataFrames, MySQL, DBInterface, ProgressMeter, DataStructures
using IniFile
using DuckDB

export DBconnect, DBclose, DBprepare, DB, @DB_str, DBsource, upload_table!
export my_read_parquet, my_write_parquet


struct DBLoginInfo
    host::String
    user::Union{Nothing, String}
    pass::Union{Nothing, String}
    dbname::Union{Nothing, String}

    function DBLoginInfo(; dbname=nothing)
        ini = read(Inifile(), joinpath(ENV["HOME"], ".my.cnf"))
        host = get(ini, "client-mariadb", "host", nothing)
        user = get(ini, "client-mariadb", "user", nothing)
        pass = get(ini, "client-mariadb", "password", nothing)
        return new(host, user, pass, dbname)
    end

    function DBLoginInfo(host, user; dbname=nothing)
        pass = askpass("Enter password for DB user $user")
        return new(host, user, pass, dbname)
    end
end


const globalconn = Vector{DBInterface.Connection}()


function DBclose()
    if length(globalconn) == 1
        DBclose(globalconn[1])
        empty!(globalconn)
    end
end
DBclose(conn::DBInterface.Connection) = DBInterface.close!(conn)



DBconnect(args...; store_global=false, kws...) =
    DBconnect(DBLoginInfo(args...; kws...), store_global=store_global)
function DBconnect(login::DBLoginInfo; store_global=false)
    conn = DBInterface.connect(MySQL.Connection, login.host, login.user, login.pass)
    isnothing(login.dbname)  ||  DB("USE $(login.dbname)", conn=conn)
    if store_global
        DBclose()
        push!(globalconn, conn)
    end
    return conn
end


function DBGlobalConnection()
    @assert length(globalconn) == 1 "No connection opened, call DBconnect(..., store_global=true)"
    return globalconn[1]
end


DBprepare(sql::AbstractString; conn=DBGlobalConnection()) = DBInterface.prepare(conn, string(sql))

DB(stmt, params...) = DBInterface.execute(stmt, params)
function DB(stmt, df::DataFrame)
    desc = split(stmt.sql)[1] * join(fill(" ", 9))
    desc = desc[1:9] * " "
    barlen = ProgressMeter.tty_width(desc, stderr, false)
    (barlen > 50)  &&  (barlen = 50)
    prog = Progress(nrow(df), desc=desc, dt=0.5, color=:light_black, barlen=barlen,
                    barglyphs=BarGlyphs('|','█', ['▏','▎','▍','▌','▋','▊','▉'],' ','|',))
    for (i, row) in enumerate(Tables.rows(df))
        ProgressMeter.update!(prog, i)
        DBInterface.execute(stmt, Tables.Row(row))
    end
    nothing
end

#=
Note: using mysql_store_result=false may produce unexpected and
unexplicable errors on the client side, which can be solved only by
closing and re-opening the connection.
=#
function DB(sql::AbstractString; store_on_client=true, conn=DBGlobalConnection())
    out = DataFrame(DBInterface.execute(conn, string(sql), mysql_store_result=store_on_client))
    if  (nrow(out) == 0)
        return nothing
    end
    if  (ncol(out) == 1)  &&
        (nrow(out) == 1)
        return out[1,1]
    end
    return out
end


function DBsource(file::AbstractString, subst::Vararg{Pair{String,String}, N}; conn=DBGlobalConnection()) where N
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
                DB(sql, conn=conn)
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
        DB(sql, conn=conn)
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
DBColumn(data::BitVector                     ) = DBColumn(data, "TINYINT    SIGNED")

function DBColumn(data::Vector{Union{Missing, Symbol}})
    i = findall(.!ismissing.(data))
    @assert length(i) > 0 "All values are missing"
    return DBColumn(data, "ENUM(" * join("'" .* sort(unique(string.(data[i]))) .* "'", ", ") * ")")
end

DBColumn(data::Vector{Symbol}) =
    return DBColumn(data, "ENUM(" * join("'" .* sort(unique(string.(data))) .* "'", ", ") * ")")


function DBColumn(data::Vector{Union{Missing, String}})
    # i = findall(.!ismissing.(data))
    # if length(i) == 0
    #     @warn "All values are missing, assuming a length of 20"
    #     maxlen = 20
    # else
    #     maxlen = maximum(length.(data[i]))
    # end
    # return DBColumn(data, "VARCHAR($(maxlen))")
    return DBColumn(data, "TEXT ASCII")
end

function DBColumn(data::Vector{String})
    # maxlen = maximum(length.(data))
    # return DBColumn(data, "VARCHAR($(maxlen))")
    return DBColumn(data, "TEXT ASCII")
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
    if col.null
        i = findall(  ismissing.(data[:, name]));
        data[i, name] .= missing
    end
    i = findall(.!ismissing.(data[:, name]))
    data[i, name] .= Int8.(data[i, name])
end



function DBColumns(data::DataFrame)
    out = OrderedDict{Symbol, DBColumn}()
    for name in Symbol.(names(data))
        out[name] = DBColumn(data[:, name])
    end
    return out
end

upload_table!(data::DataFrame, tbl_name::String; conn=DBGlobalConnection(), kw...) =
    upload_table!(data, DBColumns(data), tbl_name; conn=conn, kw...)

function upload_table!(data::DataFrame, meta::OrderedDict{Symbol, DBColumn}, tbl_name::String;
                       conn=DBGlobalConnection(),
                       create=false, temp=false, memory=false, engine=nothing, charset=nothing)
    coldefs = Vector{String}()
    for name in Symbol.(names(data))
        print("\rPreparing column $name ...")
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
    println()

    table_exists = true
    try
        # Can't use DB("show tables like '$(tbl_name)'") since it
        # doesn't work with tbl_name in the form of DB.TABLE.
        DB("SELECT * FROM $(tbl_name) LIMIT 0", conn=conn)
    catch
        table_exists = false
    end
    if !create  &&  !table_exists
        println("Table $tbl_name do not exists, forcing creation...")
        create = true
    end
    if create
        sql = "CREATE " * (temp ? "TEMPORARY" : "") * " TABLE $tbl_name"
        sql *= " ( " * join(coldefs, ", ") * ")"
        if memory
            sql *= " ENGINE=MEMORY"
        else
            isnothing(engine)  ||  (sql *= " ENGINE=$engine")
        end
        isnothing(charset)  ||  (sql *= " CHARACTER SET $charset")
        println(sql)
        DB(sql, conn=conn)
    end
    params = join(repeat("?", ncol(data)), ",")
    sql = "INSERT INTO $tbl_name VALUES ($params)"
    println(sql)
    stmt = DBprepare(sql, conn=conn)
    DB(stmt, data)
    nothing
end


function my_read_parquet(filename; maxrows=nothing)
    conn = DuckDB.DB()
    nn = DataFrame(DuckDB.execute(conn,
                                  "DESCRIBE SELECT * FROM read_parquet($(filename))"))[:, 1]
    df = DataFrame()
    limit = (isnothing(maxrows)  ?  ""  :  "LIMIT $maxrows")
    for n in nn
        df[!, Symbol(n)] = DuckDB.toDataFrame(
            DuckDB.execute(conn,
                "SELECT $n FROM read_parquet('$(filename)') $limit"))[1]
        GC.gc()
    end
    return df
end


function my_write_parquet(filename, df)
    conn = DuckDB.DB()
    DuckDB.register_table(conn, df, "tmp")
    DuckDB.execute(conn, "COPY tmp TO '$(filename)' (FORMAT PARQUET)")
end
