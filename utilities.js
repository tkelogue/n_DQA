#!/usr/bin/env node
'use strict';

//bytes converter to readable unit
/**
 *
 * @param {entier} sBytes indiquant en bytes la valeur à convertir en unité convenable
 * @param {entier} point indiquant le nombre de décimal à retourner dans la valeur
 * @returns un string indiquant la valeur dans la nouvelle unité
 */
const byteConverter = function (sBytes, point = 2) {
    if (sBytes == 0) return '0 Bytes';
    let k = 1024
        , sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
        , i = Math.floor(Math.log(sBytes) / Math.log(k));
    return parseFloat((sBytes / Math.pow(k, i)).toFixed(point)) + ' ' + sizes[i];
}

//type of value any
/**
 *
 * @param {any} t
 * @returns string indiquant le type détecté pour la valeur t.
 */
const _typeof = function (t = null) {
	if(/^(\d+)*(\d+)$/.test(t) || /^(\d+,)*(\d+)$/.test(t) || /^(\d+\.)*(\d+)$/.test(t)) return 'number'
    else return (t!=0 && !t || !t.replaceAll(' ','').length)?'undefined':typeof t
}
export {byteConverter, _typeof }