import pandas as pd
import strdif
import os

OUTPUT_FILENAME = 'consolidated.csv'
CACHED_CONSOLIDATED_FILENAME = 'cached_' + OUTPUT_FILENAME
INPUT_FILENAME = 'hist326_combined.csv'

def group(data):
    def quarter_group_func(data):
        if (data.str.contains('No Data', case=False, regex=False).any()):
            return 'No Data'
        if (data.str.contains('X', case=False, regex=False).any()):
            return 'X'
        return ''

    def choose(data):
        data = data[~pd.isnull(data)]
        if (len(data) == 0):
            return ''
        strs = data[~data.str.contains('\?|\[')]
        if (len(strs) == 0):
            return ''
        return strs.loc[strs.str.len().idxmax()]

    group_func_dict = {}
    group_func_dict['major'] = choose
    group_func_dict['Region (add later)'] = choose
    group_func_dict['Notes'] = choose
    group_func_dict = {**group_func_dict, **{key: quarter_group_func for key in data.columns[5:]}}
    return data.groupby(by=['name', 'country'], as_index=False).agg(group_func_dict)

def print_suggested_name_mapping(orig_data, data):
    already_hit = {}

    print('name_mapping = [')

    for i in range(len(data)):
        name = data['name'].iloc[i]
        if (i in already_hit or '?' in name):
            continue
        matches_raw = strdif.get_close_matches_indexes(name, data['name'], cutoff=0.8)

        matches = []
        for m in matches_raw:
            if (data.iloc[m]['country'] != data.iloc[i]['country']):
                continue
            matches.append(m)
            already_hit[m] = True

        # Find match that appears most in original data to determine what to convert to
        max_count = 0
        most_common_str = ''
        for m in matches:
            count = len(orig_data[orig_data['name'] == data.iloc[m]['name']])
            if (count > max_count and '?' not in data.iloc[m]['name'] and '[' not in data.iloc[m]['name']):
                max_count = count
                most_common_str = data.iloc[m]['name']

        if (len(matches) > 1):
            print('   { "convert": [', end='')
            for m in matches:
                print('(%d, "%s"), ' % (m, data['name'].iloc[m]), end='')
            print('], "to": "%s" },' % most_common_str)

    print(']')    

def apply_name_mapping(mapping, data):
    for m in mapping:
        for c in m['convert']:
            data.loc[c[0], 'name'] = m["to"]
    return data

data = pd.read_csv(INPUT_FILENAME)
data = data.rename(columns={'Name':'name', 'Home Country or Country Code':'country', 'Major/Program or Major Code':'major'})

# Propagate 'No Data's all the way down
for col in data.columns:
    if ((data[col] == 'No Data').any()):
        data[col] = 'No Data'

if (os.path.isfile(CACHED_CONSOLIDATED_FILENAME)):
    print('Cached consolidated data file found! Skipping groupby operation.')
    consolidated = pd.read_csv(CACHED_CONSOLIDATED_FILENAME)
else:
    print('Performing initial groupby operation...', end='', flush=True)
    consolidated = group(data)
    print('done!')

    consolidated.to_csv(CACHED_CONSOLIDATED_FILENAME, index=False)

try:
    import name_mapping
except:
    print('Did not find name mapping!')
    print_suggested_name_mapping(data, consolidated)
    exit()

print('Name mapping found!')
print('Applying name mapping to grouped data set...', end='', flush=True)
consolidated = apply_name_mapping(name_mapping.name_mapping, consolidated)
print("done!")

print('Reperfoming groupby operation following the name mapping...', end='', flush=True)
consolidated = group(consolidated)
print('done!')

print('Data set consolidated from %d lines to %d lines. Saving data to %s...' %
        (len(data), len(consolidated), OUTPUT_FILENAME), end='', flush=True)
consolidated = consolidated.rename(columns={'name':'Name', 'country':'Home Country or Country Code', 'major':'Major/Program or Major Code'})
consolidated.to_csv(OUTPUT_FILENAME, index=False)
print('done!')