#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 26 18:32:57 2021

@author: hyradus
"""
import urllib.request
from bs4 import BeautifulSoup as BS
import pathlib
import pandas as pd


def getFileUrl(df_url, ext):
    page = urllib.request.urlopen(df_url)
    soup = BS(page, 'html.parser')
    FUrl = [link.get('href') for link in soup.findAll('a') if link.get('href').endswith(ext)][0]    
    return(FUrl)

def getFile(url, dst_path):
    import ssl
    ssl._create_default_https_context = ssl._create_unverified_context
    fname = pathlib.Path(url).name
    savename = dst_path +'/'+fname
    urllib.request.urlretrieve(url, savename)
    data_dict = {'Name': fname} 
    tmp_df = pd.DataFrame.from_dict([data_dict])
    return(tmp_df)