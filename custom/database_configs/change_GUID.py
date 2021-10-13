"""
v1.1

Changelog


v1.1 - 08JUN2021
Adapted to G2CRM v0.4.564.3


"""

import sys
import os
import argparse
# from PyQt5.QtCore import *
# from PyQt5.QtGui import *

from PyQt5.QtWidgets import *
from PyQt5 import QtCore, QtGui
from PyQt5.QtGui import *
from PyQt5.QtCore import *

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
    folder_path = "/".join(argv.outputDatabase.split("/")[0:-2])
    QMessageBox.information(None, "Custom Prompt", "Changing input databases GUID from: " + folder_path)

    os.chdir(folder_path)
    directories = ["Storms", "System", "Assets", "PlanAlternative"]
    
        # ---------------- CHANGE GUID FOR INPUT DBs ----------------
    for directory in directories:
        guid = uuid.uuid4()

        try:
            # select first item in each folder
            file_path = os.path.join(folder_path, "Data", directory, os.listdir(os.path.join(folder_path, "Data", directory))[0])
            print("Changing table guid in:\n", file_path)
            print("Using GUID:", guid)
            
            # update db guid
            conn = sqlite3.connect(file_path)
            cur = conn.cursor()
            sql = "UPDATE g2crm_db_info SET db_guid = \"" + str(guid) + "\";"
            print(sql)
            cur.execute(sql)

        except Exception as e:
            conn.rollback()
            conn.close()
            print(e)
            raise(Exception)
        finally:
            conn.commit()
            conn.close()

if __name__ == "__main__":
    args, unknown = get_parser().parse_known_args()
    main(args)