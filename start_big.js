#!/usr/bin/env node
'use strict'
import os from 'os';
import path from 'path';
import { fileURLToPath } from 'url';
import open, { openApp, apps } from 'open';
import start from './index.js';
//launch browser and viewer page
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const url = __dirname + '\\frontView.html'
await open(url);

//file to analyze
const f = 5;
const files = [
    "C:/Users/KT/source/csv xls xlsx analyzer/HiH Agricultural Typologies Dataset - Haiti.xlsx"
    , "C:/Users/KT/Downloads/HiH Agricultural Typologies - Poverty Dataset - Haiti.csv"
    , "C:/Users/KT/Documents/Scott2016.xlsx"
    , "C:/Users/KT/Documents/quartiers2010.csv"
    , "C:/Users/KT/Downloads/Urban population 1990-2010.xlsx"
    , "C:/Users/KT/Downloads/20231120gleif.csv"
    , "C:/Users/KT/Downloads/Scott2016_Compilation _Analyse2016_02032017.xls"
]
const file = files[f]
let data = start(file);

