#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SCRIPT for Download LROC EDR/RDR files from PDS
@author: Giacomo Nodjoumi - g.nodjoumi@jacobs-university.de

README

Before start:

1) download LROC NAC coverage files from https://ode.rsl.wustl.edu/moon/datafile/derived_products/coverageshapefiles/moon/lro/lroc/edrnac/moon_lro_lroc_edrnac_c0a.zip
2) extract it and import shapefiles into GIS software
3) search by filter or select by location and create a new shapfile with the selection
4) Start the script and select the filtered shapefile


"""

import os

import pandas as pd
from tqdm import tqdm
from utils.GenUtils import chunk_creator, parallel_funcs, readGPKG
from utils.FileUtils import getFileUrl, getFile
import psutil
from argparse import ArgumentParser
from tkinter import Tk,filedialog

global dst_folder


def main(gpkgDF):  
    JOBS=psutil.cpu_count(logical=False)
    
    download_urls = gpkgDF['FilesURL'].tolist()
    
    csv_name = dst_folder+'/file_urls.csv'
    try:
        file_urls = pd.read_csv(csv_name, header=None)[0].tolist()
    except Exception as e:
        print(e)
        file_urls = []
        with tqdm(total=len(download_urls),
                 desc = 'Generating urls',
                 unit='File') as pbar:
            
            chunks = []
            for c in chunk_creator(download_urls, JOBS):
                chunks.append(c)
        
            for i in range(len(chunks)):
                files = chunks[i]
                results = parallel_funcs(files, JOBS, getFileUrl, ext)
                pbar.update(JOBS)
                [file_urls.append(url) for url in results]
            csv_df = pd.DataFrame(file_urls)
            csv_df.to_csv(csv_name, index=False, header=None)
        pass
    
    proc_csv = dst_folder+'/Processed.csv'
    try:
        proc_df = pd.read_csv(proc_csv)
    except Exception as e:
        print(e)
        print('Processed csv created')
        proc_df = pd.DataFrame(columns=['Name'])
    pass
        
    with tqdm(total=len(file_urls),
             desc = 'Downloading Images',
             unit='File') as pbar:
        
        filerange = len(file_urls)
        chunksize = round(filerange/JOBS)
        if chunksize <1:
            chunksize=1
            JOBS = filerange
        chunks = []
        for c in chunk_creator(file_urls, JOBS):
            chunks.append(c)
            
        for i in range(len(chunks)):
            files = chunks[i]
            lambda_f = lambda element:(os.path.basename(element).split('.')[0]) not in proc_df['Name'].to_list()
            filtered = filter(lambda_f, files)
            chunk = list(filtered)
            if len(chunk)>0:
                tmp_df = parallel_funcs(files, JOBS, getFile, dst_folder)
                for df in tmp_df:
                    proc_df = proc_df.append(df,ignore_index=True)
                proc_df.to_csv(proc_csv, index=False)
                pbar.update(JOBS)
            else:
                pbar.update(len(files))
                continue

if __name__ == "__main__":
    global ddir
    
    parser = ArgumentParser()
    parser.add_argument('--path', help='download folder path')
    parser.add_argument('--shp', help='shapefile or geopackage with orbits')
    parser.add_argument('--ext', help='extension of desired files')
    
    args = parser.parse_args()
    path = args.path
    ext = args.ext
    gpkg_file = args.shp
    
    if  path is None:
        root = Tk()
        root.withdraw()
        dst_folder = filedialog.askdirectory(parent=root,initialdir=os.getcwd(),title="Please select the folder Download folder")
        print('Working folder:', dst_folder)
    
    if  gpkg_file is None:
        root = Tk()
        root.withdraw()
        gpkg_file = filedialog.askopenfilename(initialdir = "/",title = "Select file",filetypes = (("Esri Shapefile","*.shp"),("all files","*.*")))
        print('Working folder:', gpkg_file)

    if ext is None:
        print('Please enter TIFF or tiff, PNG or png or JPG or jpg, JP2. Leave empty for JP2')    
        ixt = input('Enter input image format: ')
        if ixt == '':
            ixt = '.JP2'
        
    gpkgDF = readGPKG(gpkg_file)
    
    
    main(gpkgDF)
   



