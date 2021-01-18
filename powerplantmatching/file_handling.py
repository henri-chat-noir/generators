import os
from datetime import date, datetime
import csv
import json
from copy import deepcopy

# FILE READING / LOADING
# ======================

def load_csv_to_dict(load_csv, key_cols=1):
    """    
    Forms a dictionary from csv file based on keys in one or more columns on right.
    Presumption is that a single 'master' key value for each row exists in first n columns, set by key_cols
    Then remaining values are built into dictionary based on column headers in 1st row
    
    If number key_cols > 1, then dict key formed as tuple of those row entries, in order
    If number of (remaining) value-related columns = 1, then
        Forms a simple 'flat', single-level dictionary based on single-column key or multi-column tuple
    else:
        Form a row_dict based on column labels
    
    """
    try:
        with open(load_csv, mode='r', encoding='utf-8-sig') as read_file:
            reader = csv.reader(read_file)
    
            dict = {}
            for row_num, row in enumerate(reader):
                if row_num == 0:
                    val_keys = row[key_cols:]
                
                else: # for all remaining rows
                    # If single column for value entry, then nested dictionary would be confusing
                    if len(row) == key_cols + 1:
                        try:
                            row_vals = json.loads(row[key_cols])
                        except:
                            row_vals = row[key_cols]
 
                    else:
                        # If more than one value entry, then sets up row_vals as subdictionary
                        row_vals = {}
                        for val_col, val in enumerate(row[key_cols:]):
                            row_vals[val_keys[val_col]] = val

                    if key_cols == 1:
                        row_key = row[0]
                    else:
                        # Note zero indexing, so index 'at' key_cols point is start of values and excluded from slice
                        row_key = tuple( row[:key_cols] ) 
                    
                    dict[row_key] = row_vals

    except:
        dict = {}

    return dict

def load_json_to_dict(load_file, key_type='str'):

    print(f"Loading json to dict . . . {load_file}\n")
    try:
        with open(load_file) as json_file:
            raw_dict = json.load(json_file)
    except:
        raw_dict = {}
        
    return_dict = {}
    for key, val in raw_dict.items():
        if key_type == 'int':
            return_key = int(key)
        elif key_type == 'tup':
            return_key = eval(key)
        else:
           return_key = key

        return_dict[return_key] = val
        
    return return_dict

def load_csv_to_dict_list(read_csv, key_labels = [], has_header_row = True):
    """
    Loads CSV to a list of dictionaries, selecting columns (as per keyLabels list).
    
    If keyLabels . . .
        if nullstring,
            then all columns loaded, with keys identical to column headings as a simple list,
        else
            keys provided as a list of 2tuples, tup[0] = existing label, tup[1] equals revised labeling of keys
    
    Note: If csv does NOT have a header row, then key_labels MUST be provided as simple list of text strings
    assigned to cols 0 to length of key_labels
    """
    try:
        with open(read_csv, mode='r', encoding='utf-8-sig') as read_file:
            reader = csv.reader(read_file)
    
            dict_list = []
            row_num = 0
            label_tups = []
            for row in reader:
                if row_num == 0 and has_header_row:
                    # Build label_tups as (label, col_num) list based on different options for passing key_labels parameter
                    
                    if not key_labels:
                        # If nothing specified, then key_labels simply built from existing (entire) header row text
                        key_labels = row
                        col_num = 0
                        for label in key_labels:
                            label_tups.append((label, col_num))
                            col_num += 1

                    else:
                        if isinstance(key_labels[0], tuple):
                            # If key_labels provided as list of (col_label, key_label) tuples, then map specified columns to new keys
                            for col_label, key_label in key_labels:
                                col_num = row.index(col_label)
                                label_tups.append((key_label, col_num))
                        else:
                            # If key_labels provided as simple list (must match column labeling in csv), 
                            # then existing (specified) column_labels are simply re-used (sub-selected list of columns)
                            for col_label in key_labels:
                                col_num = row.index(col_label)
                                label_tups.append((col_label, col_num))
                else:
                    if not label_tups:
                        # If no header row, then label_tups will not have (yet) been assigned
                        # In these circumstances, key_label MUST be passed as simple list of key labels to use
                        # for each of first "n" columns left to right (based on number of entries in key_labels
                        col_num = 0
                        for key_label in key_labels:
                            label_tups.append((key_label, col_num))
                            col_num += 1
                    
                    # Build dict_list from value rows based on key information defined in label_tups
                    row_vals = {}
                    for key_label, col_num in label_tups:
                        row_vals[key_label] = row[col_num]
                        
                    dict_list.append(row_vals)
            
                row_num += 1
    except:
        dict_list = []

    return dict_list

def load_text_to_list(read_txt):

    out_list = []
    with open(read_txt, 'r', encoding='utf-8') as file_handle:
        reader = csv.reader(file_handle)
        for row in reader:
            out_list.append(row[0])

    return out_list


# FILE SAVING
# ===========
def save_dict_to_json(save_dict, save_file, key_type='str'):
    # Need to consider how openpyxl automatically recognizes Excel dates and converts to datetime objects
    # Good opportunity to re-engineers this function into recursive structure
    # For now hard-wired to allow for 3-levels deep structure of non-string dict keys

    print(f"Saving dict to json . . . {save_file}\n")
    out_dict = {}
    for key1, val1 in save_dict.items():

        out_key1 = str(key1)
        if isinstance(val1, dict):

            out_dict[out_key1] = {}
            for key2, val2 in val1.items():

                out_key2 = str(key2)
                if isinstance(val2, dict):
                    out_dict[out_key1][out_key2] = {}
                    for key3, val3 in val2.items():
                        out_key3 = str(key3)
                        out_dict[out_key1][out_key2][out_key3] = val3
                else:
                    out_dict[out_key1][out_key2] = val2

        else:
            out_dict[out_key1] = val1

    with open(save_file, 'w') as outfile:
        json.dump(out_dict, outfile)

    return

def save_flat_dict_to_csv(save_dict, save_file_spec, key_col_labels=[], val_label="", fmode='w'):
    """    
    Routine to handle single-value (text or integer) and tuple_keyed, flat dictionaries with SINGLE value
    Hence fairly specific in current form

    if fmode == 'w':
        header_row = key_col_labels + [val_label]
    else:
        header_row = None
    
    primary_keys = list( save_dict.keys() )
    
    # Problem sorting mixed-type keys that occur -- need to add code convert integer keys to strings when keys are mixed
    # sorted_primary_keys = sorted( list( save_dict.keys() ) )

    """

    with open(save_file_spec, mode=fmode, encoding='utf-8-sig') as save_file:
        csv_writer = csv.writer(save_file, delimiter=',', lineterminator='\n', quotechar='"', quoting=csv.QUOTE_ALL)
        if header_row:
            csv_writer.writerow(header_row)

        for primary_key in primary_keys:

            if isinstance(primary_key, tuple):
                row_data = list(primary_key)     
            else:
                row_data = [str(primary_key)]

            row_data.append(save_dict[primary_key])
            csv_writer.writerow(row_data)
    
    return

def save_nested_dict_to_csv(save_dict, save_file_spec, main_key_labels=[], selected_keys=[], fmode='a+'):
    
    # The main/top key elements, e.g. if tuple, have no inherent name -- must be provided by call
    header_row = main_key_labels

    primary_key_list = list( save_dict.keys() )
    sorted_primary_keys = sorted(primary_key_list)
        
    if selected_keys:
        # Allows for option to sub-select the elements to save/report (amongst secondary keys)
        secondary_keys = selected_keys
    else:
        # Just need to pick a random (first) element from dict to get list of secondary keys
        sample_key = primary_key_list[0]    
        sample_sub_dict = save_dict[sample_key]
        secondary_keys = sorted( list(sample_sub_dict.keys()))
        # secondary_keys = sorted(list( save_dict[ sorted_primary_keys[0] ].keys() ) )

    header_row += secondary_keys

    with open(save_file_spec, mode=fmode, encoding='utf-8-sig') as save_file:
        csv_writer = csv.writer(save_file, delimiter=',', lineterminator='\n', quotechar='"', quoting=csv.QUOTE_ALL)
        if header_row:
            csv_writer.writerow(header_row)

        for primary_key in sorted_primary_keys:
            if type(primary_key) == tuple:
                row_data = list(primary_key)    
            else:
                row_data = [primary_key]

            for secondary_key in secondary_keys:
                primary_info = save_dict[primary_key]
                row_data.append( primary_info.get(secondary_key))
                
            csv_writer.writerow(row_data)

    return

def save_dict_vals_to_csv(save_dict, save_file_spec, header_labels=[], fmode='a+'):

    with open(save_file_spec, mode=fmode, encoding='utf-8-sig') as save_file:
        csv_writer = csv.writer(save_file, delimiter=',', lineterminator='\n', quotechar='"', quoting=csv.QUOTE_ALL)
        if header_labels:
            csv_writer.writerow(header_labels)

        for row_data in save_dict.values():
            csv_writer.writerow(row_data)

    return

def saveTupleToCSV(dataTuple, fileSpec):

    with open(fileSpec, mode='a+', encoding='utf-8-sig') as saveFile:
        csv_writer = csv.writer(saveFile, delimiter=',', lineterminator='\n', quotechar='"', quoting=csv.QUOTE_ALL)
        csv_writer.writerow(list(dataTuple))

    return

def save_iterable_to_text(output_iterable, save_file_spec, sep_char='\n'):
    """
    Simple, single-column output of list to text file (single-column, if new_lines==True
    """
    output_list = list(output_iterable)
    with open(save_file_spec, mode='w', encoding='utf-8-sig') as save_file:
        output_string = sep_char.join(output_list)
        save_file.write(output_string)

    return

def save_obj_to_csv(save_obj, save_file_spec, key_lab="", header_labels=[], fmode='w'):
    # Works on save_obj of type 'dict' (of dictionaries) OR list of dictionaries, i.e. "dict_list" format
    # key_lab needs to be provided in order to put a text header for the top-level dictionary key column

    write_header = False
    file_exists = os.path.isfile(save_file_spec)
    if fmode == 'w' or not file_exists:
        write_header = True

    data_list = []
    if type(save_obj) == type({}):
        col_labs = [key_lab]
        key_list = list(save_obj.keys())
        first_item = save_obj[key_list[0]]
        val_labs = list(first_item.keys())
        col_labs = col_labs + val_labs

        for key, val in save_obj.items():
            list_elem = {}
            list_elem[key_lab] = key
            for val_key, col_val in val.items():
                list_elem[val_key] = col_val
            data_list.append(list_elem)
            dict_row = True

    elif isinstance(save_obj, list):
        first_row = save_obj[0]
        if type(first_row) is type({}):
            col_labs = list(save_obj[0].keys())
            dict_row = True
        else:
            write_header = False   
            dict_row = False
        
        data_list = save_obj
    
    else:
        print("saveObj of type not currently supported by this function!")
        stop = True

    try:
        with open(save_file_spec, fmode, encoding='utf-8-sig') as save_file:
            row_num = 0
            if dict_row:
                writer = csv.DictWriter(save_file, fieldnames=col_labs, delimiter=',', lineterminator='\n', quotechar='"', quoting=csv.QUOTE_ALL)
                if write_header: writer.writeheader()
                for dict in data_list:
                    writer.writerow(dict)
            else:
                writer = csv.writer(save_file, delimiter=',', lineterminator='\n', quotechar='"', quoting=csv.QUOTE_ALL)
                # work-up code to add header to lists of tuple and list of lists
                for row_data in data_list:
                    rowNum += 1
                    if row_num == 1 and header_labels:
                        writer.writerow(header_labels)
                    else:
                        writer.writerow(list(row_data))

    except IOError:
        print("I/O error") 

    return

def save_dict_vals_to_csv(save_dict, save_file_spec, header_labels=[], fmode='a+'):

    """
    Now not quite sure what this variant does -- lol
    """

    with open(save_file_spec, mode=fmode, encoding='utf-8') as save_file:
        csv_writer = csv.writer(save_file, delimiter=',', lineterminator='\n', quotechar='"', quoting=csv.QUOTE_ALL)
        if header_labels:
            csv_writer.writerow(header_labels)

        for row_data in save_dict.values():
            csv_writer.writerow(row_data)

    return

def save_flat_dict_of_iterables_to_csv(save_dict, save_file_spec, header_labels=[], item_type='str', fmode='a+'):

    """
    Saves a two-column csv file
    Presumes single-value key and saves each item of iterable into single cell
    with saving format based on item_type
    """

    with open(save_file_spec, mode=fmode, encoding='utf-8') as save_file:
        csv_writer = csv.writer(save_file, delimiter=',', lineterminator='\n', quotechar='"', quoting=csv.QUOTE_ALL)
        if header_labels:
            csv_writer.writerow(header_labels)

        for key, iterable_data in save_dict.items():
            for item in iterable_data:
                if item_type == 'str':
                    save_item = str(item)
                elif item_type == 'set':
                    save_item = set(item)

                row_data = [key, save_item]        
                csv_writer.writerow(row_data)

    return

def saveTupleToCSV(dataTuple, fileSpec):

    with open(fileSpec, mode='a+', encoding='utf-8') as saveFile:
        csv_writer = csv.writer(saveFile, delimiter=',', lineterminator='\n', quotechar='"', quoting=csv.QUOTE_ALL)
        csv_writer.writerow(list(dataTuple))

    return


# DATA OBJECT MANIPULATION
# ========================
def build_dict_from_list(key_label, dict_list):
    # Takes list (of dictionaries, typically) and constructs a nested dictionary structure
    nested_dict = {}
    for dict_row in dict_list:
        val_row = dict_row.deepcopy()
        del valRow[keyLabel]
        keyVal = dictRow[keyLabel]
        newDict[keyVal] = valRow
    
    return newDict

def serialize(val):

    if isinstance(val, (datetime, date) ):
        serialized_val = str(val)
    else:
        serialized_val = val

    return serialized_val