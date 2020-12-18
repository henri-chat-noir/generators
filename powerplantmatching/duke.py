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
from _globals import _package_data, SUB_TAG, PACKAGE_CONFIG, SUB_LINK

logger = logging.getLogger(__name__)

def add_geoposition_for_duke(df):
    """
    Returns the same pandas.Dataframe with an additional column "Geoposition"
    which concats the latitude and longitude of the powerplant in a string

    """
    if not df.loc[:, ['lat', 'lon']].isnull().all().all():
        return df.assign(Geoposition=df[['lat', 'lon']].astype(str)
                         .apply(lambda s: ','.join(s), axis=1)
                         .replace('nan,nan', np.nan))
    else:
        return df.assign(Geoposition=np.nan)

def duke_cliques(in_df, country=None, labels=['one', 'two'], showmatches=False, showoutput=False):
    """
    
    """
    duke_config_fn = "duke_find_cliques.xml"
    link_fn = "linkfile.txt"
    input_fn = "input.csv"  # This MUST match the filename spcification in duke_config.xml
    
    # Routine to add all duke_bin_dir folders to enironment path
    duke_bin_dir = _package_data('duke_binaries')
    os.environ['CLASSPATH'] = os.pathsep.join([os.path.join(duke_bin_dir, r) for r in os.listdir(duke_bin_dir)])
    
    # for fn in os.listdir(duke_bin_dir):
        # os.environ['CLASSPATH'] = path.join(duke_bin_dir, fn)
    
    
    # Set-up clean / empty working directory
    ds_name = in_df.columns.name
    work_dir = path.join(PACKAGE_CONFIG['data_dir'], SUB_TAG, ds_name)
    if not path.exists(work_dir):
        os.makedirs(work_dir)
    
    if country is not None:
        work_dir = path.join(work_dir, country)
        if not path.exists(work_dir):
            os.makedirs(work_dir)
            
    # work_dir = tempfile.mkdtemp()
    # shutil.rmtree(work_dir) # Clear tree structure if already exists
    
    # Copy relevant duke config file into working directory
    shutil.copyfile(path.join(_package_data(duke_config_fn)),
                    path.join(work_dir, duke_config_fn))

    # shutil.copyfile(_package_data(duke_config_fn), path.join(work_dir, duke_config_fn))

    message = f"Finding unit cliques for {ds_name} / {country} . . ."
    logger.debug(message)
    print(message)

    df = add_geoposition_for_duke(in_df)
    df.to_csv( path.join(work_dir, "input.csv"), index_label='id')

    # Build list of arguments to pass to Duke executable
    args = ['java', '-Dfile.encoding=UTF-8', 'no.priv.garshol.duke.Duke',
            '--linkfile='+link_fn]
    if showmatches:
        args.append('--showmatches')
        stdout = sub.PIPE
    else:
        stdout = None
    args.append(duke_config_fn)


    # Run Duke process
    try:
        run = sub.Popen(args, stderr=sub.PIPE, cwd=work_dir, stdout=stdout, universal_newlines=True)
    except FileNotFoundError:
        err = "Java was not found on your system."
        logger.error(err)
        raise FileNotFoundError(err)

    matches, stderr = run.communicate()

    logger.debug(f"Stderr: {stderr}")
    if any(word in stderr.lower() for word in ['error', 'fehler']):
        raise RuntimeError("duke failed: {}".format(stderr))

    link_spec = path.join(work_dir, link_fn)
    out_df = pd.read_csv(link_spec, encoding='utf-8', usecols=[1, 2], names=labels)
        
    logger.debug(f'Files of the duke run have been saved to {work_dir}')
        
    return out_df

def duke_link(df_A, df_B, country=None,  singlematch=False,
         showmatches=False, keepfiles=True, showoutput=False):
    """
    """

    duke_config_fn = "duke_find_links.xml"
    duke_bin_dir = _package_data('duke_binaries')

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

    shutil.copyfile(os.path.join(_package_data(duke_config_fn)),
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

    if singlematch:
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