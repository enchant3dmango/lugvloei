import os
import fnmatch

def get_yaml_config_files(directory, suffix):
    print(f'This is the direcory: {directory}')
    print(f'This is the suffix: {suffix}')
    matches = []
    for root, _, filenames in os.walk(directory):
        print(f'This is the root: {root}')
        print(f'This is the filenames: {filenames}')
        
        for filename in fnmatch.filter(filenames, f'{suffix}'):
            print(f'This is the filename: {filename}')
            matches.append(os.path.join(root, filename))
    
    print(f'This is the matches: {matches}')
    return matches
