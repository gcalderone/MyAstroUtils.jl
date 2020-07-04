using DataFrames, ODBC, ProgressMeter

export dbconnection, DBtransaction, DBstmt, DB, upload_table


const DBConn = Vector{ODBC.Connection}()

function dbconnection(conn_string; user=nothing, pass=nothing)
    if !isnothing(user)  &&  isnothing(pass)
        pass = askpass("Enter password for DB user $user")
    end
    conn = ODBC.Connection(conn_string *
                           (isnothing(user)  ?  ""  :  ";USER=" * user * (
                               isnothing(pass)  ?  ""  :  ";PWD=" * pass)))
    push!(DBConn, conn)
    nothing
end


DBtransaction(f) = ODBC.transaction(f, DBConn[end])
DBstmt(sql::AbstractString) = DBInterface.prepare(DBConn[end], string(sql))

DB(sql::AbstractString) = DBInterface.execute(DBConn[end], string(sql))
DB(stmt::ODBC.Statement, params...) = DBInterface.execute(stmt, params)
function DB(stmt::ODBC.Statement, df::DataFrame)
    DBtransaction() do
        @showprogress 0.5 for (i, row) in enumerate(Tables.rows(df))
            DBInterface.execute(stmt, Tables.Row(row))
        end
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
        elseif t == Int8
            push!(dbtype, "TINYINT SIGNED $notnull")
        elseif t == Int16
            push!(dbtype, "SMALLINT SIGNED $notnull")
        elseif t == Int32
            push!(dbtype, "INT SIGNED $notnull")
        elseif t == Int64
            push!(dbtype, "BIGINT SIGNED $notnull")
        elseif t == Bool
            push!(dbtype, "BOOLEAN $notnull")
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


function upload_table(_df::DataFrame, name; drop=true)
    (sql, df) = prepare_columns(_df)

    if drop
        DB("DROP TABLE IF EXISTS $name")
        DB("CREATE TABLE $name (" * join(sql, ", ") * ")")
    end
    params = join(repeat("?", ncol(df)), ",")
    stmt = DBstmt("INSERT INTO $name VALUES ($params)")
    DB(stmt, df)
end

