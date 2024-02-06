using PyCall, DataFrames

export read_votable, tap_query

function read_votable(filename::String)
    VOTable = pyimport("astropy.io.votable")
    votable = VOTable.parse(filename)
    table = votable.get_first_table().to_table()

    df = DataFrame()
    for col in table.colnames
        c = table.columns[col]
        if isa(c, PyObject)
            @assert string(pytypeof(c)) == "PyObject <class 'astropy.table.column.MaskedColumn'>"
            @warn "Skipping $col ..."
            display(countmap(c.mask))
        else
            @assert isa(c, Vector)
            df[!, Symbol(col)] = c
        end
    end
    return df
end


function tap_query(url::String, query::String;
                   dump_to_file=true, output_format="fits", output_file=tempname())
    @assert !isfile(output_file)
    TapPlus = pyimport("astroquery.utils.tap.core").TapPlus
    tap = TapPlus(url=url)
    job = tap.launch_job_async(query,
                               dump_to_file=dump_to_file,
                               output_format=output_format,
                               output_file=output_file)
    pybuiltin("print")(job)
    return output_file
end


