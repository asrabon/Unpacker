# standard python libraries
import argparse
from concurrent.futures import ThreadPoolExecutor
import os
import sys
import tarfile
import time
import traceback
import zipfile

# list to store the all the future objects of processing individual archives
archive_processes = []
archive_executor = ThreadPoolExecutor()
# list to store the all the future objects of processing individual file extraction from the archives
file_processes = []
file_executor = ThreadPoolExecutor()


def main(directory, recursive):
    try:
        # get the absolute path to the directory to extract archives from
        directory_path = os.path.abspath(directory)
        # extract all the archives in the path
        extract_archives(directory_path, recursive)
        global file_processes
        '''
        While there is still a file process that isn't done yet continue.
        Note: This must be done with the file processes rather than the archive
        processes because file processes can be recursive and add another archive
        process so if all file processes have finished we know the program has finished
        '''
        while file_processes:
            file_processes = [process for process in file_processes if not process.done()]
            time.sleep(.01)
        # shutdown the thread pools
        file_executor.shutdown()
        archive_executor.shutdown()
    except Exception as e:
        traceback.print_exc()
        sys.exit(-1)


def extract_archives(directory, recursive=True):
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.abspath(os.path.join(root, file))
            if zipfile.is_zipfile(file_path):
                archive_processes.append(archive_executor.submit(
                    extract_archive,
                    file_path,
                    'zip',
                    recursive
                ))
            elif tarfile.is_tarfile(file_path):
                archive_processes.append(archive_executor.submit(
                    extract_archive,
                    file_path,
                    'tar',
                    recursive
                ))


def extract_archive(archive, archive_type, recursive):
    # TODO: add in preamble in case directory already exists
    extract_folder = '{}-extracted'.format(archive)
    # make the directory for the extracted files
    os.mkdir(extract_folder)
    if archive_type == 'zip':
        archive_file = zipfile.ZipFile(archive)
        archive_files = archive_file.namelist()
    elif archive_type == 'tar':
        archive_file = tarfile.TarFile(archive)
        archive_files = archive_file.getmembers()
    for file in archive_files:
        file_processes.append(file_executor.submit(
            extract_archive_file,
            archive_file,
            file,
            extract_folder,
            archive_type,
            recursive
        ))


def extract_archive_file(archive_file, file_to_extract, extract_folder, archive_type, recursive):
    if archive_type == 'zip':
        extracted_file_path = os.path.abspath(os.path.join(extract_folder, file_to_extract))
    else:
        extracted_file_path = os.path.abspath(os.path.join(extract_folder, file_to_extract.path))
    extracted_file_folder = os.path.dirname(extracted_file_path)
    # create the directory structure before extracting otherwise exception will be thrown during extraction due to race case on other threads
    os.makedirs(extracted_file_folder, exist_ok=True)
    try:
        archive_file.extract(file_to_extract, extract_folder)
        print('Extracted File: {}'.format(extracted_file_path))
        # if the recursive flag is set check for nested archives
        if recursive and not os.path.isdir(extracted_file_path):
            if zipfile.is_zipfile(extracted_file_path):
                print('Found Nested Zip File: {}'.format(extracted_file_path))
                archive_processes.append(archive_executor.submit(
                    extract_archive,
                    extracted_file_path,
                    'zip',
                    recursive
                ))
            elif tarfile.is_tarfile(extracted_file_path):
                print('Found Nested Tar File: {}'.format(extracted_file_path))
                archive_processes.append(archive_executor.submit(
                    extract_archive,
                    extracted_file_path,
                    'tar',
                    recursive
                ))
    except FileNotFoundError as e:
        print('File({}): Failed to extract due to {}'.format(extracted_file_path, e))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-dir', '--directory', help='Directory to extract archives from.', required=True)
    parser.add_argument('-r', '--recursive', action='store_true', help='To enable recursive archive extraction used for nested archives')
    args = parser.parse_args()
    main(args.directory, args.recursive)
