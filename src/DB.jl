using DataFrames, MySQL, DBInterface, ProgressMeter

export DBconnect, DBtransaction, DBprepare, DB, @DB_str, DBsource, upload_table


const DBConn = Vector{DBInterface.Connection}()
DBconnect() = DBConn[end]

function DBconnect(host; user=nothing, passwd=nothing, dbname=nothing)
    DBInterface.close!.(DBConn)
    empty!(DBConn)

    if !isnothing(user)  &&  isnothing(passwd)
        passwd = askpass("Enter password for DB user $user")
    end
    conn = DBInterface.connect(MySQL.Connection, host, user, passwd)

    # "Driver={MariaDB};SERVER=127.0.0.1"
    #conn = ODBC.Connection(host *
    #                       (isnothing(user)  ?  ""  :  ";USER=" * user * (
    #                           isnothing(passwd)  ?  ""  :  ";PWD=" * passwd)))
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
    desc = split(stmt.sql)[1] * " "
    barlen = ProgressMeter.tty_width(desc, stderr)
    (barlen > 50)  &&  (barlen = 50)
    prog = Progress(nrow(df), desc=desc, dt=0.5, color=:light_black, barlen=barlen,
                    barglyphs=BarGlyphs('|','█', ['▏','▎','▍','▌','▋','▊','▉'],' ','|',))
    DBtransaction() do
        for (i, row) in enumerate(Tables.rows(df))
            update!(prog, i)
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


function prepare_columns(_df::DataFrame)
    df = deepcopy(_df)
    colnames = Vector{Symbol}()
    for name in names(df)
        if isa(df[1, name], AbstractVector)
            for i in 1:length(df[1, name])
                df[!, Symbol(name, i)] .= getindex.(df[:, name], i)
                push!(colnames, Symbol(name, i))
            end
        else
            push!(colnames, Symbol(name))
        end
    end
    select!(df, colnames)

    dbtype = Vector{String}()
    for i in 1:ncol(df)
        tn = eltype(df[[], i])
        if isa(tn, Union)
            t = nonmissingtype(tn)
            @assert isa(t, DataType) "Type not supported: $tn"
            hasnull = true
        else
            t = tn
            hasnull = false
        end

        notnull = (hasnull  ?  ""  :  "NOT NULL")
        if t == Float32
            hasnull  ||   allowmissing!(df, i)
            df[(.!ismissing.(df[:, i]))  .&  (.!isfinite.(df[:, i])), i] .= missing
            push!(dbtype, "FLOAT")
        elseif t == Float64
            hasnull  ||   allowmissing!(df, i)
            df[(.!ismissing.(df[:, i]))  .&  (.!isfinite.(df[:, i])), i] .= missing
            push!(dbtype, "DOUBLE")
        elseif t == UInt8
            push!(dbtype, "TINYINT UNSIGNED $notnull")
        elseif t == Int8
            push!(dbtype, "TINYINT SIGNED $notnull")
        elseif t == UInt16
            push!(dbtype, "SMALLINT UNSIGNED $notnull")
        elseif t == Int16
            push!(dbtype, "SMALLINT SIGNED $notnull")
        elseif t == UInt32
            push!(dbtype, "INT UNSIGNED $notnull")
        elseif t == Int32
            push!(dbtype, "INT SIGNED $notnull")
        elseif t == Int64
            push!(dbtype, "BIGINT SIGNED $notnull")
        elseif t == Bool
            # MySQL.jl package does not yet support INSERT prepared statements with BOOLEAN data type
            push!(dbtype, "TINYINT SIGNED $notnull")
            df[!, i] .= Int8.(df[!, i])
        elseif t == Symbol
            df[!, i] .= string(df[:, i])
            push!(dbtype, "ENUM(" * join("'" .* sort(unique(string.(df[:, i]))) .* "'", ", ") * ") $notnull")
        elseif t == String
            push!(dbtype, "VARCHAR(" * string(maximum(length.(df[:, i]))) * ") $notnull")
        else
            error("Type not supported: $t")
        end
    end

    sql = "`" .* string.(names(df)) .* "` " .* dbtype
    return (sql, df)
end


function upload_table(_df::DataFrame, tbl_name; drop=true, temp=false, memory=false)
    (sql, df) = prepare_columns(_df)

    if drop
        DB("DROP TABLE IF EXISTS $tbl_name")
        DB("CREATE " * (temp ? "TEMPORARY" : "") * " TABLE $tbl_name (" * join(sql, ", ") * ") " * (memory ? "ENGINE=MEMORY" : ""))
    end
    params = join(repeat("?", ncol(df)), ",")
    stmt = DBprepare("INSERT INTO $tbl_name VALUES ($params)")
    DB(stmt, df)
end

