import sys
import os
import argparse
from PyQt4.QtCore import *
from PyQt4.QtGui import *
import sqlite3
import uuid


def get_parser():
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(description='')
    parser.add_argument("-o", "--outputDatabase",
                        dest="outputDatabase",
                        help="G2CRM output database file. (.sqlite)",
                        metavar="FILE")
    parser.add_argument("-i", "--studyDatabase",
                        dest="studyDatabase",
                        help="G2CRM input database file. (.sqlite)",
                        metavar="FILE")  
    parser.add_argument("-a", "--assetDatabase",
                        dest="assetDatabase",
                        help="G2CRM asset database file. (.sqlite)",
                        metavar="FILE")
    parser.add_argument("-p", "--planAlternativeDatabase",
                        dest="planAlternativeDatabase",
                        help="G2CRM plan alternative database file. (.sqlite)",
                        metavar="FILE")
    parser.add_argument("-t", "--stormDatabase",
                        dest="stormDatabase",
                        help="G2CRM storm database file. (.sqlite)",
                        metavar="FILE")
    parser.add_argument("-s", "--systemDatabase",
                        dest="systemDatabase",
                        help="G2CRM system database file. (.sqlite)",
                        metavar="FILE")
    parser.add_argument("-r", "--projection",
                        dest="projection",
                        help="G2CRM study projection",
                        metavar="INT")
    parser.add_argument("-m", "--modeledStormSet",
                        dest="modeledStormSet",
                        help="G2CRM current modeled storm set")
    parser.add_argument("-c", "--runConditions",
                        dest="runConditions",
                        help="G2CRM current run conditions")
    return parser


def main(argv):
    app = QApplication(sys.argv)

    try:
        # update db guid
        conn = sqlite3.connect(argv.stormDatabase)
        # select max(StormNumber) from MAStormPrecipitationVolume
        cur = conn.cursor()

        cur.execute("UPDATE ModeledStormLocationDetail SET SignificantWaveHeight=0")
        cur.execute("UPDATE ModeledStormLocationSummary SET MaximumWaveHeight=0, TimeOfPeakWaveHeight=0")
        

    except Exception as e:
        conn.rollback()
        conn.close()
        print(e)
        raise(Exception)
    finally:
        conn.commit()
        conn.close()

    QMessageBox.information(None, "Removed All Waves!", "Successfully removed all wave data from the storm database.")


if __name__ == "__main__":
    args, unknown = get_parser().parse_known_args()
    main(args)