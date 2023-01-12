def clean(file_path="")-> None:
    """Remove duplicate URLs from file"""
    with open(file_path, 'r') as dirty_file:
        file = dirty_file.read()
        dirty_file = file.split()

    clean = list(dict().fromkeys(dirty_file))
    with open(file_path, 'w') as clean_file:
        clean_file.write('')
    with open(file_path, 'a') as clean_file:
        for c in clean:
            clean_file.write(''.join([' ', c]))
    return None
