import sys
from qtpy.QtWidgets import QApplication, QWidget, QVBoxLayout, QPushButton,QFileDialog, QLabel, QMessageBox, QLineEdit, QHBoxLayout
import math
import numpy as np
import h5py as h5
import json
import numpy as np
from scipy.interpolate import interp1d
from datetime import datetime

class MultiInputFileApp(QWidget):
    
    
    def interp_hydrograph(self,y, t, tq):
        """
            Linear interpolation of signal values at query times.
            Parameters
            y : array-like
                Values of original signal
            t : array-like of datetime
                Time values of original signal
            tq : array-like of datetime
                Query time points
    
            Returns
            -------
            yq : np.ndarray
Interpolated values at tq
        """# Convert datetime arrays to numeric (seconds since t[0])
        t0 = t[0]
        t_sec  = np.array([(ti - t0).total_seconds() for ti in t])
        tq_sec = np.array([(tqi - t0).total_seconds() for tqi in tq])
        
                # Build linear interpolator
        f = interp1d(t_sec, y, kind="linear", fill_value="extrapolate")
        
                # Evaluate at query points
        yq = f(tq_sec)
        return yq
        
        
    def __init__(self):
        super().__init__()

        # Use a list to hold the paths for the three input files
        self.input_filenames = [None, None, None]
        self.output_filename = None

        self.setWindowTitle("Bias and Water Level Adjuster")
        self.setGeometry(300, 300, 500, 300)

        # Main layout
        layout = QVBoxLayout()

        # --- Create UI elements for 3 Input Files using a loop ---
        self.input_labels = []

        # Create a button for each input file
        btn = QPushButton(f"Select ADCIRC Input File", self)
        
        # Use a lambda function to pass the index (0, 1, or 2) to the handler
        btn.clicked.connect(lambda checked, index=0: self.select_input_file(index))
        layout.addWidget(btn)

        # Create and store a label for each input file path
        lbl = QLabel(f"Input {1}: [Not Selected]", self)
        self.input_labels.append(lbl)
        layout.addWidget(lbl)
        # Create a button for each input file
        btn = QPushButton(f"Select STWAVE Input File", self)
        
        # Use a lambda function to pass the index (0, 1, or 2) to the handler
        btn.clicked.connect(lambda checked, index=1: self.select_input_file(index))
        layout.addWidget(btn)

        # Create and store a label for each input file path
        lbl = QLabel(f"Input {2}: [Not Selected]", self)
        self.input_labels.append(lbl)
        layout.addWidget(lbl)
        # Create a button for each input file
        btn = QPushButton(f"Select ADCIRC Peaks File", self)
        
        # Use a lambda function to pass the index (0, 1, or 2) to the handler
        btn.clicked.connect(lambda checked, index=2: self.select_input_file(index))
        layout.addWidget(btn)

        # Create and store a label for each input file path
        lbl = QLabel(f"Input {3}: [Not Selected]", self)
        self.input_labels.append(lbl)
        layout.addWidget(lbl)

        # --- UI for Output File ---
        self.btn_set_output = QPushButton("Set Output File", self)
        self.btn_set_output.clicked.connect(self.set_output_file)
        layout.addWidget(self.btn_set_output)
        
        self.lbl_output_path = QLabel("Output File: [Not Selected]", self)
        layout.addWidget(self.lbl_output_path)
        
        hbox = QHBoxLayout()
        hbox.addWidget(QLabel("Enter Relative Bias:", self))
        
        self.rel_bias = QLineEdit(self)
        self.rel_bias.setPlaceholderText("e.g., 0.5")
        self.rel_bias.setText("1.0")
        hbox.addWidget(self.rel_bias)
        
        layout.addLayout(hbox)

        hbox = QHBoxLayout()
        hbox.addWidget(QLabel("Enter Absolute Bias:", self))
        
        self.abs_bias = QLineEdit(self)
        self.abs_bias.setPlaceholderText("e.g., 0.5")
        self.abs_bias.setText("1.0")
        hbox.addWidget(self.abs_bias)
        
        layout.addLayout(hbox)

        # --- Process Button and Status Label ---
        self.btn_process = QPushButton("Process Files", self)
        self.btn_process.clicked.connect(self.process_files)
        self.btn_process.setEnabled(False) # Disable until all files are selected
        layout.addWidget(self.btn_process)
        
        self.lbl_status = QLabel("Status: Please select all 4 files.", self)
        layout.addWidget(self.lbl_status)

        self.setLayout(layout)

    def select_input_file(self, index):
        """Selects an input file for the given index (0, 1, or 2)."""
        file_name, _ = QFileDialog.getOpenFileName(self, f"Select Input File {index+1}", "", "Text Files (*.h5);;All Files (*)")

        if file_name:
            self.input_filenames[index] = file_name
            self.input_labels[index].setText(f"Input {index+1}: {file_name}")
            self.check_all_files_selected()

    def set_output_file(self):
        """Sets the path for the single output file."""
        file_name, _ = QFileDialog.getSaveFileName(self, "Set Output File", "", "Text Files (*.h5);;All Files (*)")

        if file_name:
            self.output_filename = file_name
            self.lbl_output_path.setText(f"Output File: {file_name}")
            self.check_all_files_selected()

    def check_all_files_selected(self):
        """Checks if all files are selected and enables the Process button."""
        # 'all()' checks if every item in the list is not None
        if all(self.input_filenames) and self.output_filename:
            self.btn_process.setEnabled(True)
            self.lbl_status.setText("Status: Ready to process.")
        else:
            self.btn_process.setEnabled(False)
            self.lbl_status.setText("Status: Please select all required files.")

    def process_files(self):
        """Reads from the 3 input files and writes their combined content to the output file."""



        #Raw CHS file from old NACCS data. 
        h5f1 = h5.File(self.input_filenames[0],'r')#ADCIRC File
        h5f2 = h5.File(self.input_filenames[1],'r')#STWAVE File
        h5f3 = h5.File(self.input_filenames[2],'r')#ADCIRC Peaks File
        h5f4 = h5.File(self.output_filename,'w')
        storm_names=list(h5f2.keys())
        #bias_a=-0.224458442
       # bias_r=-0.5813436
        bias_a=float(self.abs_bias.text())
        bias_r=float(self.rel_bias.text())

        attributes=h5f2.attrs
        nancount=0
        #===============================
        #writing to .h5 file
        file_3a = 'Water Elevation'
        file_3b = 'yyyymmddHHMM'

        file_3c = 'Mean Wave Direction'
        file_3d = 'Mean Wave Period'
        file_3e = 'Peak Period'
        file_3f = 'Wind Direction'
        file_3g = 'Wind Magnitude'
        file_3h = 'Zero Moment Wave Height'





        for i, obj in enumerate(storm_names):

            groupadc=h5f1[obj]
            groupstw=h5f2[obj]
            grouppeaks=h5f3[obj]

            nans=math.isnan(groupstw['Water Elevation'][0])
            if nans:
            	nancount=nancount+1    
            else:

                datesadc = [datetime.strptime(str(int(x)), "%Y%m%d%H%M") for x in groupadc['yyyymmddHHMM']]
                datesstw = [datetime.strptime(str(int(x)), "%Y%m%d%H%M") for x in groupstw['yyyymmddHHMM']]
                wavestoadcirc=self.interp_hydrograph(groupstw['Zero Moment Wave Height'][:],datesstw,datesadc)
                wavestoadcirc[wavestoadcirc<0]=0
                grp2 = h5f4.create_group(obj)
                surge=groupadc['Water Elevation'][:]
                peaksurge=grouppeaks['Water Elevation'][0]
                surge[surge==max(surge)]=peaksurge
                biasadj=np.divide(1,np.power(np.add(np.divide(1,np.power(bias_a,2)),np.divide(1,np.power(np.multiply(bias_r,np.abs(surge)+.01),2))),0.5))
                surge=np.subtract(surge,np.multiply(np.sign(bias_a),biasadj))
                dset3a = grp2.create_dataset(file_3a, data = surge)
                dset3b = grp2.create_dataset(file_3b, data = groupadc['yyyymmddHHMM'])
                dset3c = grp2.create_dataset(file_3c, data = np.zeros(len(dset3a)))
                dset3d = grp2.create_dataset(file_3d, data = np.zeros(len(dset3a)))
                dset3e = grp2.create_dataset(file_3e, data = np.zeros(len(dset3a)))
                dset3f = grp2.create_dataset(file_3f, data = np.zeros(len(dset3a)))
                dset3g = grp2.create_dataset(file_3g, data = np.zeros(len(dset3a)))
                dset3h = grp2.create_dataset(file_3h, data = wavestoadcirc)
                h5f4.attrs['CHS Data Format'] = attributes['CHS Data Format'].decode("utf-8")
                h5f4.attrs['Grid Name'] ='NACCS'
                h5f4.attrs['Latitude Units'] = attributes['Latitude Units'].decode("utf-8")
                h5f4.attrs['Longitude Units'] =attributes['Longitude Units'].decode("utf-8")
                h5f4.attrs['Project'] ='Baltimore'
                h5f4.attrs['Region'] ='Baltimore'
                h5f4.attrs['Save Point ID'] =int(attributes['Save Point ID'])
                h5f4.attrs['Save Point Latitude'] =float(attributes['Save Point Latitude'])
                h5f4.attrs['Save Point Longitude'] =float(attributes['Save Point Longitude'])
                h5f4.attrs['Vertical Datum'] =attributes['Vertical Datum'].decode("utf-8")

        #	    h5f1.attrs['Grid Name'] =attributes['Grid Name'][0]
        #	    h5f1.attrs['Latitude Units'] = attributes['Latitude Units']
        #	    h5f1.attrs['Longitude Units'] =attributes['Longitude Units']
        #	    h5f1.attrs['Project'] =attributes['Project']
        #	    h5f1.attrs['Region'] =attributes['Region']
        #	    h5f1.attrs['Save Point ID'] =attributes['Save Point ID']
        #	    h5f1.attrs['Save Point Latitude'] =attributes['Save Point Latitude']
        #	    h5f1.attrs['Save Point Longitude'] =attributes['Save Point Longitude']
        #	    h5f1.attrs['Vertical Datum'] =attributes['Vertical Datum']
        		
        		#group two attributes
                grp2.attrs['Record Interval'] = groupadc.attrs['Record Interval']
                grp2.attrs['Record Interval Units'] = groupadc.attrs['Record Interval Units'].decode("utf-8")
                grp2.attrs['Save Point Depth'] = float(groupstw.attrs['Save Point Depth'])
                grp2.attrs['Save Point Depth Units'] = groupstw.attrs['Save Point Depth Units'].decode("utf-8")
                grp2.attrs['Steric Level'] = groupadc.attrs['Steric Level'].decode("utf-8")
                grp2.attrs['Storm ID'] =  groupstw.attrs['Storm ID'].decode("utf-8")
                grp2.attrs['Storm Name'] = groupstw.attrs['Storm Name'].decode("utf-8")
                grp2.attrs['Storm Type'] = 'TS'
        		
        		#group three-a attributes
                dset3a.attrs['Model Variable'] = 'eta'
                dset3a.attrs['Units'] = 'm'
        		
        		#group three-b attributes
                dset3b.attrs['Units'] = 'yyyymmddHHMM'
        		
        		#group three-c through three-h attributes
                dset3c.attrs['Model Variable'] = 'alpham'
                dset3c.attrs['Units'] = 'deg'
        		
                dset3d.attrs['Model Variable'] = 'TM'
                dset3d.attrs['Units'] = 'sec'
        		
                dset3e.attrs['Model Variable'] = 'Tp'
                dset3e.attrs['Units'] = 'sec'
        		
                dset3f.attrs['Model Variable'] = 'UDIR'
                dset3f.attrs['Units'] = 'deg'
        		
                dset3g.attrs['Model Variable'] = 'U'
                dset3g.attrs['Units'] = 'm/s'
        		
                dset3h.attrs['Model Variable'] = 'Hmo'
                dset3h.attrs['Units'] = 'm'



        h5f1.close()
        h5f2.close()
        h5f3.close()
        h5f4.close()

        #===============================










if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = MultiInputFileApp()
    ex.show()
    sys.exit(app.exec_())