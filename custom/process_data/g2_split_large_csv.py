import os
from PyQt4.QtGui import *
from PyQt4.QtCore import *
import pandas as pd
import sys

def get_parser():
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(description='')
    parser.add_argument("-o", "--outputDatabase",
                        dest="outputDatabase",
                        help="G2CRM output database file. (.sqlite)",
                        metavar="FILE")
    return parser


def main(argv):
    app = QApplication(sys.argv)
    file = QFileDialog.getOpenFileName(None, "Select CSV File", os.path.dirname(argv.outputDatabase), "Comma-Separated Values Files (*.csv)")
    # output = QFileDialog.getSaveFileName(None, "Storms Export Excel File", directory, "Excel File (*.xlsx)")
    output = QFileDialog.getExistingDirectory(None, "Select Directory")
    prefix, ok = QInputDialog.getText(None, 'Input Dialog', 'Enter output files prefix:')
    chunk_size, ok = QInputDialog.getInteger(None, 'Input Dialog', 'Enter the max number of observations for each output file:', value=500000)
    file = str(file)
    output = str(output)
    prefix = str(prefix)

    try:
        for i,chunk in enumerate(pd.read_csv(file, chunksize=chunk_size, encoding='latin')):
            chunk.reset_index().to_csv(os.path.join(output, prefix +'_' + str(i) + '.csv'), index=False) # reset index in case of multilevel index
    except Exception as e:
        QMessageBox.information(None, "File Chunking Failure", "Operation failed to execute - Refer to error log")
        print(e)
        raise e
    QMessageBox.information(None, "File Chunking Success", "Successfully chunked " + (file) + " and wrote files to: " + (output))


if __name__ == "__main__":
    args, unknown = get_parser().parse_known_args()
    main(args)