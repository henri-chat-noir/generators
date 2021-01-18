# -*- coding: utf-8 -*-

import logging
import os
from os import path
# from os import path, listdir, environ, pathsep, mkdir
import subprocess as sub
import shutil
import tempfile
import pandas as pd
import numpy as np
from _globals import SUB_TAG, PACKAGE_CONFIG, SUB_LINK, SUB_CLEAN, SUB_DIAG
import _globals as glob

logger = logging.getLogger(__name__)

def duke_cliques(in_df, country=None, show_output=True):
    """
    
    """
    duke_config_fn = "duke_find_cliques.xml"
    link_fn = "linkfile.txt"
    input_fn = "input.csv"  # This MUST match the filename spcification in duke_config.xml
    
    # Routine to add all duke_bin_dir folders to enironment path
    duke_bin_dir = glob.package_data('duke_binaries')
    os.environ['CLASSPATH'] = os.pathsep.join([os.path.join(duke_bin_dir, r) for r in os.listdir(duke_bin_dir)])
    
    # Set-up clean / empty working directory
    ds_name = in_df.columns.name
    work_dir = path.join(PACKAGE_CONFIG['data_dir'], SUB_TAG, ds_name)
    if not path.exists(work_dir):
        os.makedirs(work_dir)
    
    if country is not None:
        work_dir = path.join(work_dir, country)
        if not path.exists(work_dir):
            os.makedirs(work_dir)
            
    message = f"Finding unit cliques for {ds_name} / {country} . . ."
    logger.debug(message)
    print(message)

    # Save dataframe to be processed into working directory
    in_df.to_csv(path.join(work_dir, input_fn), index_label='projectID', encoding='utf8')

    # Copy relevant duke config file into working directory
    shutil.copyfile(path.join(glob.package_data(duke_config_fn)),
                    path.join(work_dir, duke_config_fn))

    # Build list of arguments to pass to Duke executable
    args = ['java', '-Dfile.encoding=UTF-8', 'no.priv.garshol.duke.Duke',
            '--linkfile='+link_fn]
    
    # args.append('--progress') # Show progress report while running
    args.append('--showmatches') # Show matches while running

    # args.append('--verbose')
    # MATCH 0.9983154777071377
    # ID: '18WCTJON1-123-06', NAME: 'castejo', FUELTYPE: 'natural gas', COUNTRY: 'Spain', CAPACITY: '424.9', GEOPOSITION: '42.17080120000001,-1.6894331999999999', 
    # ID: '18WCTJON2-123-0Z', NAME: 'castejo', FUELTYPE: 'natural gas', COUNTRY: 'Spain', CAPACITY: '378.9', GEOPOSITION: '42.17080120000001,-1.6894331999999999', 
    # Matching record ID: '18WCTJON2-123-0Z', NAME: 'castejo', FUELTYPE: 'natural gas', COUNTRY: 'Spain', CAPACITY: '378.9', GEOPOSITION: '42.17080120000001,-1.6894331999999999',  found 79 candidates

    # args.append('--testdebug')
    # args.append('--profile')
    # args.append('--showdata')
    # args.append('--pretty')

    args.append(duke_config_fn)

    # stdout = None
    stdout = sub.PIPE
    
    # Run Duke process
    try:
        run = sub.Popen(args, stderr=sub.PIPE, cwd=work_dir, stdout=stdout, universal_newlines=True)

    except FileNotFoundError:
        err = "Java was not found on your system."
        logger.error(err)
        raise FileNotFoundError(err)

    stdout, stderr = run.communicate()
    if show_output:
        print(stdout)

    logger.debug(f"Stderr: {stderr}")
    if any(word in stderr.lower() for word in ['error', 'fehler']):
        raise RuntimeError("duke failed: {}".format(stderr))

    link_spec = path.join(work_dir, link_fn)
    out_df = pd.read_csv(link_spec, encoding='utf-8', usecols=[1, 2], names=['one', 'two'])
        
    logger.debug(f'Files of the duke run have been saved to {work_dir}')
        
    return out_df

def duke_link(df_A, df_B, country=None, showmatches=False, keepfiles=True, showoutput=False):
    """
    """

    duke_config_fn = "duke_find_links.xml"
    duke_bin_dir = glob.package_data('duke_binaries')

    # Routine to add all duke_bin_dir folders to enironment path
    os.environ['CLASSPATH'] = \
        os.pathsep.join([os.path.join(duke_bin_dir, r)
                         for r in os.listdir(duke_bin_dir)])
    
    ds_A = df_A.columns.name
    ds_B = df_B.columns.name
    pair_label = " - ".join([ds_A, ds_B])

    work_dir = path.join(PACKAGE_CONFIG['data_dir'], SUB_LINK, pair_label)
    if not path.exists(work_dir):
        os.makedirs(work_dir)

    if country is not None:
        work_dir = path.join(work_dir, country)
        if not path.exists(work_dir):
            os.makedirs(work_dir)

    shutil.copyfile(os.path.join(glob.package_data(duke_config_fn)),
                    os.path.join(work_dir, duke_config_fn))

    logger.debug(f"Comparing files: {pair_label}")

    # Get this additional data added earlier in process
    df_A = add_geoposition_for_duke(df_A)
    df_B = add_geoposition_for_duke(df_B)

    # Note that hard-wired filenames here must align with what is set in Duke config file
    df_A.to_csv( os.path.join(work_dir, "file_A.csv"), index_label='id')

    # due to index unity (see https://github.com/larsga/Duke/issues/236)
    shift_B_by = df_A.index.max()+1
    df_B.index += shift_B_by
    df_B.to_csv( os.path.join(work_dir, "file_B.csv"), index_label='id')
    df_B.index -= shift_B_by

    args = ['java', '-Dfile.encoding=UTF-8', 'no.priv.garshol.duke.Duke',
            '--linkfile=linkfile.txt']
    
    args.append('--singlematch')
    if showmatches:
        args.append('--showmatches')
        stdout = sub.PIPE
    else:
        stdout = None
    args.append(duke_config_fn)
        
    print(f"Executing Duke Java process for {pair_label} / {country} . . .")
    try:
        run = sub.Popen(args, stderr=sub.PIPE, cwd=work_dir, stdout=stdout,
                        universal_newlines=True)
     
    except FileNotFoundError:
        err = "Java was not found on your system."
        logger.error(err)
        raise FileNotFoundError(err)

    matches, stderr = run.communicate()

    if showmatches:
        print(matches)

    logger.debug("Stderr: {}".format(stderr))
    if any(word in stderr.lower() for word in ['error', 'fehler']):
        raise RuntimeError("duke failed: {}".format(stderr))

    link_spec = os.path.join(work_dir, 'linkfile.txt')
    col_labels = [ds_A, ds_B, 'scores']
    res = pd.read_csv(link_spec, encoding='utf-8', usecols=[1, 2, 3], names=col_labels)

    res.iloc[:, 1] -= shift_B_by
    logger.debug(f"Files of the duke run are kept in {work_dir}")

    return res

def duke_pair_data(id1=None, id2=None, country=None, test_sub=None):
    """
    
    """
    # duke_config_fn = "duke_pairs_data.xml"
    duke_config_fn = "duke_find_cliques.xml"
    # link_fn = "linkfile.txt"
    input_fn = "input.csv"  # This MUST match the filename spcification in the Duke config.xml
    
    # Set-up clean / empty working directory
    work_dir = path.join(PACKAGE_CONFIG['data_dir'], SUB_DIAG)
    if not path.exists(work_dir):
        os.makedirs(work_dir)
    
    if test_sub is not None:
        work_dir = path.join(work_dir, test_sub)
        if not path.exists(work_dir):
            os.makedirs(work_dir)
            
    # Copy relevant duke config file into working directory
    shutil.copyfile(path.join(glob.package_data(duke_config_fn)),
                    path.join(work_dir, duke_config_fn))

    # Copy over latest concatenated 'all ids' file from cleaned folder
    # Since pairs analysis uses same parameters as 'cliques', then 'input.csv' must be filename
    all_ids_spec = glob.set_path("ALL_IDS_clean.csv", SUB_CLEAN)
    shutil.copyfile(all_ids_spec, path.join(work_dir, 'input.csv'))

    # Routine to add all duke_bin_dir folders to enironment path
    duke_bin_dir = glob.package_data('duke_binaries')
    os.environ['CLASSPATH'] = os.pathsep.join([os.path.join(duke_bin_dir, r) for r in os.listdir(duke_bin_dir)])
    
    message = f"\nFinding pair info for {id1} to {id2} . . ."
    logger.debug(message)
    print(message)

    # Build list of arguments to pass to Duke executable
    args = ['java', '-Dfile.encoding=UTF-8', 'no.priv.garshol.duke.DebugCompare', duke_config_fn, id1, id2]
    
    # Run Duke process
    try:
        run = sub.Popen(args, stderr=sub.PIPE, cwd=work_dir, stdout=sub.PIPE, universal_newlines=True)

    except FileNotFoundError:
        err = "Java was not found on your system."
        logger.error(err)
        raise FileNotFoundError(err)

    stdout, stderr = run.communicate()
    # print("Stdout: ", stdout)

    logger.debug(f"Stderr: {stderr}")
    if any(word in stderr.lower() for word in ['error', 'fehler']):
        raise RuntimeError("duke failed: {}".format(stderr))
        print("Duke Failed")

    return stdout

def duke_report_options():
    
    # duke_config_fn = "duke_find_cliques.xml"
    # link_fn = "linkfile.txt"
    # input_fn = "input.csv"  # This MUST match the filename spcification in duke_config.xml
    
    # Routine to add all duke_bin_dir folders to enironment path
    duke_bin_dir = glob.package_data('duke_binaries')
    os.environ['CLASSPATH'] = os.pathsep.join([os.path.join(duke_bin_dir, r) for r in os.listdir(duke_bin_dir)])
    
    # Set-up clean / empty working directory
    
    # Build list of arguments to pass to Duke executable
    # args = ['java', '-Dfile.encoding=UTF-8', 'no.priv.garshol.duke.Duke', '--linkfile='+link_fn]
    # args = ['java', 'no.priv.garshol.duke.Duke']
    args = ['java', 'no.priv.garshol.duke.DebugCompare']




    # args = ['java', 'no.priv.garshol.duke.matchers.PrintMatchListener']
    
    # Run Duke process
    try:
        run = sub.Popen(args)

    except FileNotFoundError:
        err = "Java was not found on your system."
        logger.error(err)
        raise FileNotFoundError(err)
        
    return