using DataFrames, MySQL, DBInterface, ProgressMeter

export DBtransaction, DBprepare, DB, upload_table


const DBConn = Vector{DBInterface.Connection}()
current_db_conn() = DBConn[end]

function set_db_connection(host; user=nothing, passwd=nothing)
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
    nothing
end


DBtransaction(f) = MySQL.transaction(f, current_db_conn())
DBprepare(sql::AbstractString) = DBInterface.prepare(current_db_conn(), string(sql))

DB(sql::AbstractString) = DataFrame(DBInterface.execute(current_db_conn(), string(sql)))
DB(stmt, params...) = DataFrame(DBInterface.execute(stmt, params))
function DB(stmt, df::DataFrame)
    DBtransaction() do
        @showprogress 0.5 for (i, row) in enumerate(Tables.rows(df))
            DBInterface.execute(stmt, Tables.Row(row))
        end
    end
    nothing
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
        elseif t == Int8
            push!(dbtype, "TINYINT SIGNED $notnull")
        elseif t == Int16
            push!(dbtype, "SMALLINT SIGNED $notnull")
        elseif t == Int32
            push!(dbtype, "INT SIGNED $notnull")
        elseif t == Int64
            push!(dbtype, "BIGINT SIGNED $notnull")
        elseif t == Bool
            # MySQL.jl package does not yet support INSERT preparaed statements with BOOLEAN data type
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


function upload_table(_df::DataFrame, tbl_name; drop=true)
    (sql, df) = prepare_columns(_df)

    if drop
        DB("DROP TABLE IF EXISTS $tbl_name")
        DB("CREATE TABLE $tbl_name (" * join(sql, ", ") * ")")
    end
    params = join(repeat("?", ncol(df)), ",")
    stmt = DBprepare("INSERT INTO $tbl_name VALUES ($params)")
    DB(stmt, df)
end

