import zipfile
from pathlib import Path


def zip_files(output_filename: Path,
              list_of_filepaths: list) -> None:
    """Write a list of files to the provided output_filename

    :param output_filename: path to the new ZIP file
    :type output_filename: Path
    :param list_of_filepaths: list of filepaths to put into the zip file
    :type list_of_filepaths: list
    :return: None
    """
    compression = zipfile.ZIP_DEFLATED

    zf = zipfile.ZipFile(output_filename, mode="w")
    for file in list_of_filepaths:
        zf.write(file, file.name, compress_type=compression)

    zf.close()
