#!/usr/bin/env python

import os
import sys
from os.path import join, getsize, exists
from operator import itemgetter
import cPickle as pickle
import gzip
import shutil
import argparse

testRun = True
createSpanWarning = False

baseDirectory = ""
outputDirectory = ""

discFolderName = "Backup-"

ignoredFiles = [".DS_Store", ".localized"]
bytesInGbyte = 1000000000
bytesInMbyte = 1000000

discSize = 4.69*bytesInGbyte

errors = []

def handle_error(the_error):
    errors.append(the_error)
    print "\n***** ERROR *****", the_error, "\n"

# TODO - Modify this to find large files and split them first
def record_structure(root_dir):
    relative_path_start = len(root_dir) + 1
    collection = []

    for path, dirs, files in os.walk(root_dir):
        files_in_dir = []

        for file_name in files:
            try:
                file_size = getsize(join(path, file_name))
            except Exception as e:
                error = join(path, file_name) + " " + str(e) + " record_structure()"
                handle_error(error)
            else:
                files_in_dir.append([file_name, file_size])

        if len(files_in_dir):
            collection.append([path[relative_path_start:], files_in_dir])

    return collection


def fill_disc(file_list, disc_size):
    disc_remaining = disc_size
    size_to_stop_caring = (disc_size*.0015)
    retry_count = 0
    max_retries = 3
    disc_content = []
    spanned = []
    remaining = []
    # filesForSplitting = []

    for directory in sorted(file_list, key=itemgetter(0), reverse=False):

        dirs_assigned_files = []
        dirs_next_files = []
        dirs_span_warnings = []

        for file_entry in directory[1]:
            if file_entry[0] in ignoredFiles:
                error = join(directory[0], file_entry[0]) + " File Ignored"
                handle_error(error)

            elif (retry_count >= max_retries) and (disc_remaining < size_to_stop_caring):
                dirs_next_files.append(file_entry)
                if len(dirs_assigned_files):
                    dirs_span_warnings.append(file_entry)

            elif disc_remaining > file_entry[1]:
                dirs_assigned_files.append(file_entry)
                disc_remaining -= file_entry[1]
                # print "Added", join(directory[0],file[0])

            else:
                file_too_big_for_disc = disc_size < file_entry[1] # (4*bytesInGbyte < file_entry[1]) or
                file_cant_fit_on_disc = disc_remaining < file_entry[1]
                full_file_path = join(directory[0], file_entry[0])

                if file_too_big_for_disc:
                    error = join(directory[0], file_entry[0])+ " " + str(file_entry[1]/bytesInMbyte) + "MB, FILE TO BE SPLIT NOT INCLUDED IN BACKUP"
                    handle_error(error)
                    # dirs_next_files.append(file_entry)
                    dirs_span_warnings.append(file_entry)

                elif file_cant_fit_on_disc:
                    dirs_next_files.append(file_entry)
                    dirs_span_warnings.append(file_entry)
                    retry_count += 1
                    # print "    No Room - Marked for next disc", full_file_path

                else:
                    error = "***** Something I didn't Foresee ***** " + full_file_path
                    handle_error(error)

        if len(dirs_assigned_files):
            disc_content.append([directory[0], dirs_assigned_files])

        if len(dirs_next_files):
            remaining.append([directory[0], dirs_next_files])

        if len(dirs_span_warnings):
            spanned.append([directory[0], dirs_span_warnings])

    return disc_content, spanned, remaining


def create_discs(file_list):
    # global discSize
    compiled_discs = []
    disc_number = 1
    content, spans, remain = fill_disc(file_list, discSize)
    compiled_discs.append({'disc_number': disc_number, 'disc_contents': content, 'span_warnings': spans})
    while len(remain):
        disc_number += 1
        # # TEMP
        # if disc_number == 3:
        #     discSize = 4.4*bytesInGbyte
        # if disc_number >= 4:
        #     discSize = 4.2*bytesInGbyte
        # # TEMP
        content, spans, remain = fill_disc(remain, discSize)
        compiled_discs.append({'disc_number': disc_number, 'disc_contents': content, 'span_warnings': spans})
    return compiled_discs


def create_links(disc_number, passed_contents):
    if len(passed_contents) == 0:
        return

    for directory in passed_contents:
        current_directory_path = join(outputDirectory, discFolderName + str(disc_number), directory[0])

        if not exists(current_directory_path):
            try:
                print "Make folder", outputDirectory
                if not testRun:
                    os.makedirs(current_directory_path)
            except Exception as e:
                error = outputDirectory + " " + str(e) + " create_links() creating output directory"
                handle_error(error)

        for file_item in directory[1]:
            source_file = join(baseDirectory, directory[0], file_item[0])
            output_file = join(current_directory_path, file_item[0])

            try:
                print "    Linking -", source_file, output_file
                if not testRun:
                    os.link(source_file, output_file)
            except Exception as e:
                error = str(e) + " " + source_file + " " + output_file + " create_links->os.link"
                handle_error(error)


def create_span_warnings(disc_number, span_warns):
    if len(span_warns) == 0 or not createSpanWarning:
        return
    for directory in span_warns:
        for file_item in directory[1]:
            output_file = join(outputDirectory, discFolderName + str(disc_number), directory[0], file_item[0]) + ".spanned"
            try:
                print "    Creating span warning file", output_file
                if not testRun:
                    open(output_file, 'a').close()
            except Exception as e:
                error = str(e) + " " + output_file + " create_span_warnings"
                handle_error(error)


def pickle_discs(compiled_discs):
    if not testRun:
        pickle.dump(compiled_discs, gzip.open(join(outputDirectory, "discs.pickle"), 'wb'))
        shutil.copyfile(__file__, join(outputDirectory, "script.py"))


def display_catalog(compiled_discs):
    print "\nCATALOGUE FILE!!\n"
    entire_backup_size = 0
    for disc in compiled_discs:
        title = "* Disc # " + str(disc['disc_number']) + "/" + str(len(compiled_discs)) + " *"
        print "*"*len(title)
        print title
        print "*"*len(title)
        total_disc_size = 0
        for directory in disc['disc_contents']:
            print "    - " + directory[0]
            for file_item in directory[1]:
                total_disc_size += file_item[1]
                print "      + "+file_item[0]
        print "\nTotal Disc", disc['disc_number'], "Size:", total_disc_size/bytesInMbyte, "MB"
        entire_backup_size += total_disc_size
    print "Total backup size : ", entire_backup_size / bytesInMbyte, "MB"



def create_error_log():
    for error in errors:
        print error
        if not testRun:
            with open(join(outputDirectory, "error.log"), 'a') as error_log:
                error_log.write(str(error)+"\n")


def main():
    file_list = record_structure(baseDirectory.rstrip(os.sep))
    discs = create_discs(file_list)
    for disc in discs:
        create_links(disc['disc_number'], disc['disc_contents'])
        create_span_warnings(disc['disc_number'], disc['span_warnings'])
    pickle_discs(discs)
    display_catalog(discs)
    create_error_log()

if __name__ == '__main__':
    run_main = True
    help_text = "run with --help to show options"
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--source", help="The Directory you wish to backup")
    parser.add_argument("-d", "--destination", help="The destination directory (must not be a child of the source directory)")
    parser.add_argument("-z", "--disc-size", help="Medium size in MB")
    parser.add_argument("-c", "--display-catalog", help="Display catalog in script directory")
    parser.add_argument("-t", "--test", help="Debug operation without making changes to disc",action='store_true')
    parser.add_argument("-n", "--disc-name", help="The name of the disc")

    args = parser.parse_args()

    if args.display_catalog:
        print "Displaying catalog"
        file = pickle.load(gzip.open(args.display_catalog, "rb"))
        display_catalog(file)
        run_main = False
    else:
        if args.source == None:
            print "No source", help_text
            run_main = False
        else:
            baseDirectory = args.source.rstrip(os.sep)

        if args.destination == None:
            print "No destination", help_text
            run_main = False
        elif args.source + "/" in args.destination + "/":
            print "destination cannot be inside source", help_text
            run_main = False
        else:
            outputDirectory = args.destination.rstrip(os.sep)

        if args.disc_size == None:
            print "No Disc Size"
            run_main = False
        else:
            discSize = int(args.disc_size) * bytesInMbyte

        if args.disc_name != None:
            discFolderName = args.disc_name + "-"

        if args.test:
            testRun = True
            print "Making this a test"
        else:
            testRun = False

        if run_main:
            main()