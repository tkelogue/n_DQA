#!/usr/bin/env node
'use strict';
import { _typeof, byteConverter } from './utilities.js';

//generate data file QA
/**
 *
 * @param df un objet de class {DataFrame} du module dataframe-js
 * @returns un objet avec les resultats de l'analyse de qualité (QA)
 * @example :
 * {
 *      nCols: entier indiquant le nombre de colonnes;
 *      nRows: entier indiquant le nombre de lignes;
 *      fields: objet rapportant :
 *              - liste des colonnes et les types détectés;
 *              - la liste et le nombre de cas de duplications de lignes;
 *              - la distribution de fréquence de chacune des colonnes;
 *              - la valeur modale;
 *              - les statisques descriptives (min, max, avg, var, std, etc) si colonne numérique
 * }
 */
export default async function df_analyzer(df) {
    let analyzer = {
        nCols: df.dim()[1],
        nRows: df.dim()[0],
        getCols: getCols(df),
        //dupRows: getDuplicates(df),
        //colsStats: colStats(df)
    };

    //columns list and type of fields
    function getCols(df) {
        /*
            Cette fonction reçoit un dataframe-js df
            et retourne la liste des colonnes et le type du contenu.

        */
        let t00 = Date.now()
        let ctypes = {}
            , descriptive = {};
        df.listColumns().map(f => {
            let type = df.select(f).toArray().map(e => _typeof(e[0]))
            ctypes[f] = [...new Set(type)];
            if (ctypes[f][0] == 'number') {
                descriptive[f] = df.stat.stats(f)
            }
            else descriptive[f] = 'n/a'
        })
        return ({ cols: ctypes, descriptive: descriptive })
    }

    //detect how many and list all duplication cases
    function getDuplicates(df) {
        let t00 = Date.now();
        let gdf = df.groupBy(...df.listColumns()).aggregate(group => group.count())
            .rename('aggregation', 'duplication');
        gdf = gdf.filter(row => row.get('duplication') >= 2);
        console.log('getDuplicates:' + ((Date.now() - t00) / 1000))
        return ({
            n: gdf.count(),
            rows: gdf.toCollection()
        })
    }

    //field values frequencies distribution
    //return a object { fieldName: [values, absolute Freq, relative Freq], ... }
    function colFreq(df) {
        let t00 = Date.now()
        let col_Freq = {}
            , col_relFreq = {}
            , cols_list = df.listColumns();

        for (let k = 0; k < cols_list.length; k++) {
            let col = cols_list[k]
                , nb = df.select(col).count()
                , val = df.groupBy(col).aggregate((group) => group.count()).rename('aggregation', 'effectif');
            val = val.withColumn('freq', (row) => ((row.get('effectif') / nb) * 100).toFixed(2))
            col_Freq[col] = val.toArray();
        }
        return col_Freq
    }

    //field descriptive stats
    function colStats(df) {
        let t00 = Date.now();
        let fDist = colFreq(df)
            , stats = {};
        for (const k in fDist) {
            stats[k] = {
                mode: {
                    freq: null,
                    valeur: []
                }
            }
            let mode = Math.max(...fDist[k][2].map(e => parseFloat(e)))
            for (let fk = 0; fk < fDist[k][0].length - 1; fk++) {
                if (fDist[k][2][fk] == ('' + mode)) stats[k]['mode']['valeur'].push(fDist[k][0][fk])
            }
            stats[k]['mode']['freq'] = mode;
        }

        return [fDist, stats]
    }

    return analyzer;
}
export { df_analyzer }