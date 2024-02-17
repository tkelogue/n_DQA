#!/usr/bin/env node
'use strict';
import * as dfd from "danfojs-node";
import { DataFrame } from 'dataframe-js';
import fs from 'fs';
import { df_analyzer } from './df_analyzer.js';
import * as U from './utilities.js';

export default async function start(_File) {
    const responseFile = _File.split('.')[0] + '.json';
    let stat = fs.statSync(_File)
        , t0 = Date.now()
        , ext = (_File).split(".").slice(-1)[0]
        , df;

    if (["xls", "xlsx"].includes(ext)) {
        df = await dfd.readExcel(_File)
        df = await new DataFrame(df.values, df.$columns)
    }
    else if (ext == "csv") {
        df = await DataFrame.fromCSV(_File)
    }
    else if (ext == "json") {
        df = await DataFrame.fromJSON(_File)
    }
    else {
        df = null;
        console.log("Not supported format. Capabilities available only for csv, xls, xlsx, json")
        return
    }

    //Start file analysis process
    let data = await df_analyzer(df);
    //console.log(_File + ': ' + ((Date.now() - t0) / 1000) + ' secondes ->' + data.nRows + 'x' + data.nCols)

    //put together the result of the process
    data = Object.assign({}, {
        nRows: data.nRows,
        nCols: data.nCols,
        cols: data.getCols.cols,
        colsDescriptive: data.getCols.descriptive,
        duplicates: data.dupRows,
        colFrequencies: data.colsStats??[0],
        modales: data.colsStats??[1],
        filename: _File,
        format: ext,
        size: U.byteConverter(stat.size),
        update: new Date(stat.ctimeMs).toLocaleString(),
        delai_sec: ((Date.now() - t0) / 1000) + ' secondes'
    })
    let dk = ['nRows', 'nCols', 'filename', 'format', 'size', 'update', 'delai_sec']
    for (const k in dk) console.log(dk[k], JSON.stringify(data[dk[k]]), '\n\n')
    fs.writeFileSync(responseFile, JSON.stringify(data))
    console.log("les résultats déposés dans ce fichier: ", responseFile)
    console.log('delai_sec ', ((Date.now() - t0) / 1000) + ' secondes')
    return data
}

