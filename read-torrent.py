#!/usr/bin/env python3
import sys
import bencodepy
import pandas as pd

def parse_torrent(file_path):
    """Read and decode the torrent file using bencode."""
    with open(file_path, 'rb') as f:
        data = f.read()
    return bencodepy.decode(data)

def get_files_list(torrent):
    """
    Return the list of file entries.
    For multi-file torrents, returns the list in info['files'].
    For single-file torrents, creates a single-entry list.
    """
    info = torrent.get(b'info', torrent)
    if b'files' in info:
        return info[b'files']
    else:
        # Single file torrent: wrap the info dict into a list
        return [info]

def format_file_entry(file_entry):
    """
    Extract and format the file path and size from a file entry.
    In multi-file torrents, the path is a list of byte strings.
    """
    if b'path' in file_entry:
        # Decode each component and join with '/'
        path = "/".join(component.decode('utf-8') for component in file_entry[b'path'])
    else:
        path = file_entry[b'name'].decode('utf-8')
    size = file_entry[b'length']
    return path, size

def human_readable_size(size):
    """Convert file size from bytes to a human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"

def main(torrent_file):
    torrent = parse_torrent(torrent_file)
    files = get_files_list(torrent)
    
    # Sort files by length (file size) in descending order.
    sorted_files = sorted(files, key=lambda f: f[b'length'], reverse=True)
    
    # Store results in a DataFrame
    data = []
    for entry in sorted_files:
        path, size = format_file_entry(entry)
        data.append((path, human_readable_size(size)))
    
    df = pd.DataFrame(data, columns=["File Path", "Size"])
    return df

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: {} <torrent_file>".format(sys.argv[0]))
        sys.exit(1)
    
    torrent_file = sys.argv[1]
    df = main(torrent_file)
    print(df)
    df.to_csv('torrent_files.tsv', index=False, sep='\t')
