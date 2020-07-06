"""
tmc_summarizer.cli.py
---------------------

This module wraps ``write_summary_file()``
into a command-line-interface (CLI).

Usage: Summarize raw data
-------------------------

    $ TMC summarize my/data/

Usage: Kick off a GUI
-------------------------

    $ TMC gui

"""

import click
import os
import sys
import subprocess

from .summarize import write_summary_file
import tmc_summarizer.other_code.PySimpleGUI as sg


def open_file(filename):
    if sys.platform == "win32":
        os.startfile(filename)
    else:
        if sys.platform == "darwin":
            opener = "open"
        else:
            opener = "xdg-open"

        subprocess.call([opener, filename])


@click.group()
def main():
    """
    (T)urning (M)ovement (C)ount Summarizer.

    Import raw TMC .xls files and export a nicely-formatted
    .xlsx summary file.
    """
    pass


@main.command()
@click.argument("input_folder")
@click.option(
    "--output_folder", "-o",
    help="""Optional folder where the output
    summary file will be stored. Defaults to
    the input_folder.
    """
)
@click.option(
    "--geocode_helper", "-g",
    help="""Optional text that will increase
    the geocoding precision. e.g. 'Bristol PA'
    """
)
def summarize(input_folder, output_folder, geocode_helper):
    """
    Summarize TMC data via CLI (command line interface).

    User must specify a valid input_folder.

    Only files that follow the TMC-specific naming
    convention will be processed.
    """

    write_summary_file(input_folder, output_folder, geocode_helper)


@main.command()
def gui():
    """
    Summarize TMC data via GUI (graphical user interface).

    Uses PySimpleGUI to navigate the user's local filesystem.

    TODO: Update this with the geocode_helper
    """

    sg.theme('Dark Blue 3')

    while True:

        # Open a pop-up window for user input

        layout = [[sg.Text("Please select a folder with .xls TMC files")],
                  [sg.Input(), sg.FolderBrowse()],
                  [sg.OK(), sg.Cancel()]]

        event, values = sg.Window('TMC Summarizer', layout).read()

        # Read the value that the user provides
        input_folder = values[0]

        # Kill the app if user closes the window or clicks Cancel
        if event == sg.WIN_CLOSED or event == 'Cancel':
            break

        else:
            # If the user provides a non-blank input folder, run the script
            if input_folder != "":
                summary_file = write_summary_file(input_folder)

                # Pop the new Excel summary file open in Microsoft Excel
                open_file(summary_file)
                break

            else:
                # Force the user back to the main GUI if they submit an empty folder path
                sg.Popup("You did not select a folder. Please try again.")
