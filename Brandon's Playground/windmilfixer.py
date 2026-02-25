###############################################################################
# Windmilfixer.py
# Purpose: to make corrections to the NISC Windmil export STD file for 
#     consumption by OSI

###############################################################################
# GLOBAL DECLARATIONS
###############################################################################

# Import declarations
import csv
import time
import shutil
import zipfile
import os
import glob

###############################################################################
# Configurations
###############################################################################

# Log file in current directory
logfile = 'logfile.txt'
# Default input file in current directory
inputfile = 'input.std'
# Default output file in current directory
outputfile = 'output.std'

# Flag for copying from the GIS. 
# 1 = copy from the GIS and back to the GIS.
# 0 = Use input.std and output.std in the current directory
COPYFROMTOGIS = 1

# Location of the zip file containing the STD file on the GIS server (input ZIP file)
inputzipfile = '//12083gistc01/ivue_mapping_staking/master_database/windmil/gs12083_windmil_export.zip'
# Location to save and name to give the fixed STD file on the GIS server (output STD file)
outputfilefinal = '//12083gistc01/ivue_mapping_staking/master_database/windmil/gs12083_windmil_export_for_OSI.std'

openptfile = 'openpoints.txt'
nodefile = 'nodes.txt'

###############################################################################
# Constants and global variables
###############################################################################

# Constants to define node types
NODE = '8'
OHSPAN = '1'
UGSPAN = '3'
SOURCE = '9'
OCDEV = '10'
SWITCH = '6'
TRANS = '5'

# Declare global constants for the columns in the STD comma delimited file. First column is 0, second column is 1, etc.
NAME = 0
ETYPE = 1
PHASE = 2
PARENT = 3
XL = 5 
YL = 6
GUID = 49
PARENTGUID = 50
# Specific to OH or UG Line
XS = 31
YS = 32
# Specific to nodes
PARENTA = 27
PARENTB = 28
PARENTC = 29
# Specific to overcurrent devices
OSTATUSA = 11
OSTATUSB = 12
OSTATUSC = 13
# Specific to switches
SWSTATUS = 8
SWPARTNER = 10

# Declare global constants for Phasing codes
PH_A = '1'
PH_B = '2'
PH_C = '3'
PH_AB = '4'
PH_AC = '5'
PH_BC = '6'
PH_ABC = '7'

# Global variables

# Listing of the STD elements to keep in memory
elements = list()
# Search Indices
name_index = []
x_index = []
parent_index = []
outfile_index = []

# Processed switch list
switches_processed = []

# Number of switches inserted into the STD file
generatedSwCount = 1
generatedNodeCount = 1

###############################################################################

###############################################################################
def copy_extract(input_zip_url):
    # Copy the original zip file from the network drive to a local directory
    write_log("Copying file from GIS: " + input_zip_url)
    shutil.copy(input_zip_url, 'original.zip')
    # Extract contents of the original zip file
    write_log("Extracting...")
    shutil.rmtree('extracted')
    with zipfile.ZipFile('original.zip', 'r') as zip_ref:
        zip_ref.extractall('extracted')
    # Clean up temporary extracted directory and local copy of the original zip file
    write_log("Cleaning up...")
    os.remove('original.zip')
    files = glob.glob("extracted/*.std")
    if files:
        write_log("Found STD file: " + files[0])
        return files[0]

def copy_back(input_file_url, output_file_url):
    # Copy the original zip file from the network drive to a local directory
    write_log("Copying file to GIS: " + output_file_url)
    shutil.copy(input_file_url, output_file_url)
    
###############################################################################
# Read the STD file into a two dimensional array and provide indices for 
# the element name, x position, and parent name
# file_path: the full path name of the STD file
# returns: columns[], name_index, x_index, parent_index
###############################################################################
def read_STD_file(file_path):
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        # Read the csv into a list
        data = list(csv_reader)
        # Create search indices for NAME, XL, and PARENT columns
        name_index = sorted(range(len(data)), key=lambda i: data[i][NAME])
        x_index = sorted(range(len(data)), key=lambda i: data[i][XL])
        parent_index = sorted(range(len(data)), key=lambda i: data[i][PARENT])
    return data, name_index, x_index, parent_index

###############################################################################
# Searches the data array for a match using the sort index and indexed column
###############################################################################
def find_record(data, sorted_index, search_value, search_column_index=0):
    # Binary search for the record using the sorted index
    left = 0
    right = len(sorted_index) - 1
    while left <= right:
        mid = (left + right) // 2
        idx = sorted_index[mid]
        if data[idx][search_column_index] == search_value:
            while data[idx][search_column_index] == search_value:
                mid = mid - 1
                idx = sorted_index[mid]
            mid = mid + 1
            idx = sorted_index[mid]
            return data[idx], idx, mid  # Return the record and the index and the sorted index
        elif data[idx][search_column_index] < search_value:
            left = mid + 1
        else:
            right = mid - 1
    return None, idx, mid  # Record not found but return the index of the closest value

###############################################################################
# Save the data to an output CSV file
###############################################################################
def write_array_to_csv(data_array, file_path):
    with open(file_path, 'wb') as file:
        csv_writer = csv.writer(file)
        for i in range(len(outfile_index)):
            csv_writer.writerow(data_array[outfile_index[i]])
        
###############################################################################
# Write to the logfile and echo to the terminal
###############################################################################
def write_log(line):
    # Write the line to the file
    print(line)
    logfile.write(line + '\n')

###############################################################################
# Gets the open or closed status of an over current device
# Returns zero if all phases are open
###############################################################################
def over_current_status(swEl):
    if swEl[OSTATUSA] == '1' or swEl[OSTATUSA] == '2':
        return 1
    elif swEl[OSTATUSB] == '1' or swEl[OSTATUSB] == '2':
        return 1
    elif swEl[OSTATUSC] == '1' or swEl[OSTATUSC] == '2':
        return 1
    else:
        return 0
        
###############################################################################
# Finds the element with the matching x, y, phase, without the name or the 
# parent name equal to curEl
###############################################################################
def find_span_XYN(curEl):
    matchEl, idx, mid = find_record(elements, x_index, curEl[XL], XL)
    found = 0
    if matchEl != None:
        while found == 0 and matchEl[XL] == curEl[XL]:
            if matchEl[ETYPE] == UGSPAN or matchEl[ETYPE] == OHSPAN:
                if matchEl[XL] == curEl[XL] and matchEl[YL] == curEl[YL]:
                    if curEl[NAME] != matchEl[NAME] and curEl[PARENT] != matchEl[NAME]:
                        if curEl[PHASE] == matchEl[PHASE]:
                            found = 1
            mid = mid + 1
            idx = x_index[mid]
            matchEl = elements[idx]
        mid = mid - 1
        idx = x_index[mid]
        matchEl = elements[idx]
        if found == 1:
            return matchEl, idx
        else:
            return None, idx
    else:
        return None, idx

###############################################################################
# Calculates the outfile index for an insert at idx
###############################################################################
def get_outfile_index(idx):
    for index, element in enumerate(outfile_index):
        if element == idx:
            return index
    return -1
        
###############################################################################
# Processes overcurrent devices. If the overcurrent device is open, this 
# subroutine adds a closed switch to tie the open overcurrent device to the span 
# it would close to.
###############################################################################
def process_overcurrent_devices():
    # This function modifies global variable generatedSwCount
    global generatedSwCount
    # Traverse the elements looking for over current devices
    write_log('Processing open over current devices...')
    for curEl in elements:
        if curEl[ETYPE] == OCDEV:
            # If the over current device is open
            if over_current_status(curEl) == 0:
                # Find a matching element
                matchEl, idx = find_span_XYN(curEl)
                if matchEl != None:
                    # Insert a closed switch between the current element and the matching element
                    write_log('Open Device ' + curEl[NAME] + ' matching line ' + matchEl[NAME])
                    # Define the switch
                    fake_sw_id = 80000 + generatedSwCount
                    fake_sw_a_id = 80000 + generatedSwCount * 2
                    fake_sw_b_id = fake_sw_a_id + 1
                    fake_sw_a = ['fake_sw_' + str(generatedSwCount) + '-A','6',curEl[PHASE],matchEl[NAME],'',curEl[XL],curEl[YL],'','C',
                        str(fake_sw_id), 'fake_sw_' + str(generatedSwCount) + '-B', '','','','','','','','','','','','','','','','',
                        '','','','','','','','','','','','','','','','','','','','','','{' + str(fake_sw_b_id) + '}', 
                        '{' + str(fake_sw_a_id) + '}', matchEl[GUID]]
                    fake_sw_b = ['fake_sw_' + str(generatedSwCount) + '-B','6',curEl[PHASE],curEl[NAME],'',curEl[XL],curEl[YL],'','C',
                        str(fake_sw_id), 'fake_sw_' + str(generatedSwCount) + '-A', '','','','','','','','','','','','','','','','',
                        '','','','','','','','','','','','','','','','','','','','','','{' + str(fake_sw_a_id) + '}', 
                        '{' + str(fake_sw_b_id) + '}', curEl[GUID]]                    
                    # Insert the Switch and keep up with the output file index order
                    elements.append(fake_sw_a)
                    outfile_index.insert(get_outfile_index(idx + 1), len(elements) - 1)
                    elements.append(fake_sw_b)
                    outfile_index.insert(get_outfile_index(idx + 2), len(elements) - 1)                    
                    # Increment the generated switch count for the next switch
                    generatedSwCount = generatedSwCount + 1

#######################################################################################
# Finds nodes, switches, or over current devices that match up on an x and y coordinate
#######################################################################################
def find_node_sw_ocd(x,y):
    matchEl, idx, mid = find_record(elements, x_index, x, XL)
    found = 0
    if matchEl != None:
        while found == 0 and matchEl[XL] == x:
            if matchEl[ETYPE] == NODE or matchEl[ETYPE] == SWITCH or matchEl[ETYPE] == OCDEV:
                if matchEl[XL] == x and matchEl[YL] == y:
                    found = 1
            mid = mid + 1
            idx = x_index[mid]
            matchEl = elements[idx]
        mid = mid - 1
        idx = x_index[mid]
        matchEl = elements[idx]
        if found == 1:
            return matchEl, idx
        else:
            return None, idx
    else:
        return None, idx

###############################################################################
# Process Nodes. Resolve any issues with nodes referencing lines that are not
# coincident with the node. Add additional nodes as necessary for bidirectional
# lines.
###############################################################################
def process_nodes():
    global generatedNodeCount
    write_log('Processing nodes...')
    # Traverse the elements looking for over current devices
    for idx in range(len(elements)):
        curEl = elements[idx]
        if curEl[ETYPE] == NODE:
            # Take advantage of the fact that the file is in a specific order when looking for the child and the parent elements
            # Assume the child element immediately follows the current element
            childEl = elements[idx + 1]
            # If the order is unusual, look for the child element
            if childEl[PARENT] != curEl[NAME]:
                childEl, childidx, childmid = find_record(elements, parent_index, curEl[NAME], PARENT)
                if childEl == None:
                    write_log("Error: Child not found for node " + curEl[NAME])
            write_log("Node: " + curEl[NAME] + " with child " + childEl[NAME])
            # Remember the old phase parents    
            iniparA = curEl[PARENTA]
            iniparB = curEl[PARENTB]
            iniparC = curEl[PARENTC]
            # Look up the phase parents, unfortunately they can be anywhere in the file
            parA = None
            parB = None
            parC = None
            if curEl[PARENTA] != '': 
                parA, parAidx, parAmid = find_record(elements, name_index, curEl[PARENTA], NAME)
            if curEl[PARENTB] != '':
                parB, parBidx, parBmid = find_record(elements, name_index, curEl[PARENTB], NAME)
            if curEl[PARENTC] != '':
                parC, parCidx, parCmid = find_record(elements, name_index, curEl[PARENTC], NAME)
            # Initialize for the child node parents in case of new node or adding phases to node
            nodeparA = ''
            nodeparB = ''
            nodeparC = ''
            if childEl[PHASE] == PH_A or childEl[PHASE] == PH_AB or childEl[PHASE] == PH_AC or childEl[PHASE] == PH_ABC:
                nodeparA = childEl[NAME]        
            if childEl[PHASE] == PH_B or childEl[PHASE] == PH_AB or childEl[PHASE] == PH_BC or childEl[PHASE] == PH_ABC:
                nodeparB = childEl[NAME]
            if childEl[PHASE] == PH_C or childEl[PHASE] == PH_AC or childEl[PHASE] == PH_BC or childEl[PHASE] == PH_ABC:
                nodeparC = childEl[NAME]
            # Track what needs to be updated
            update_parent_flag = 0
            update_parenta_flag = 0
            update_parentb_flag = 0
            update_parentc_flag = 0
            # Update each phase parent if needed
            if parA != None:
                if (parA[XL] != curEl[XL] or parA[YL] != curEl[YL]) and (parA[XS] != curEl[XL] or parA[YS] != curEl[YL]):
                    if curEl[PARENT] == curEl[PARENTA]:
                        update_parent_flag = 1
                    write_log("Updating A-Phase Parent from " + curEl[PARENTA] + " to " + childEl[NAME]) 
                    curEl[PARENTA] = childEl[NAME]
                    nodeparA = iniparA
                    update_parenta_flag = 1
            if parB != None:
                if (parB[XL] != curEl[XL] or parB[YL] != curEl[YL]) and (parB[XS] != curEl[XL] or parB[YS] != curEl[YL]):
                    if curEl[PARENT] == curEl[PARENTB]:
                        update_parent_flag = 1
                    write_log("Updating B-Phase Parent from " + curEl[PARENTB] + " to " + childEl[NAME]) 
                    curEl[PARENTB] = childEl[NAME]
                    nodeparB = iniparB
                    update_parentb_flag = 1
            if parC != None:
                if (parC[XL] != curEl[XL] or parC[YL] != curEl[YL]) and (parC[XS] != curEl[XL] or parC[YS] != curEl[YL]):
                    if curEl[PARENT] == curEl[PARENTC]:
                        update_parent_flag = 1
                    write_log("Updating C-Phase Parent from " + curEl[PARENTC] + " to " + childEl[NAME]) 
                    curEl[PARENTC] = childEl[NAME]
                    nodeparC = iniparC
                    update_parentc_flag = 1
            # Update the parent and parent guid if needed
            if update_parent_flag == 1:
                if parA != None and update_parenta_flag == 0:
                    write_log("Updating parent from " + curEl[PARENT] + " to " + curEl[PARENTA])
                    curEl[PARENT] = curEl[PARENTA]
                    curEl[PARENTGUID] = parA[GUID]
                elif parB != None and update_parentb_flag == 0:
                    write_log("Updating parent from " + curEl[PARENT] + " to " + curEl[PARENTB])
                    curEl[PARENT] = curEl[PARENTB]
                    curEl[PARENTGUID] = parB[GUID]
                elif parC != None and update_parentc_flag == 0:
                    write_log("Updating parent from " + curEl[PARENT] + " to " + curEl[PARENTC])
                    curEl[PARENT] = curEl[PARENTC]
                    curEl[PARENTGUID] = parC[GUID]
            # If the node was edited, double check to see if the edited node is coincident with the lines. If not, we need to be more robust
            if (update_parenta_flag == 1) or (update_parentb_flag == 1) or (update_parentc_flag == 1):
                if (childEl[XL] != curEl[XL] or childEl[YL] != curEl[YL]) and (childEl[XS] != curEl[XL] or childEl[YS] != curEl[YL]):
                    # Let's be more robust
                    # Make sure the child element has the correct parent 
                    spanParNode, nodeIdx = find_node_sw_ocd(childEl[XS], childEl[YS])
                    if spanParNode != None:
                        write_log("Changing " + childEl[NAME] + " parent from " + childEl[PARENT] + " to " + spanParNode[NAME])
                        childEl[PARENT] = spanParNode[NAME]
                        # Check that all phases are there
                        if childEl[PHASE] != spanParNode[PHASE]:
                            write_log("Updating phase for " + spanParNode[NAME])
                            if (childEl[PHASE] == PH_A or childEl[PHASE] == PH_AB or childEl[PHASE] == PH_AC or childEl[PHASE] == PH_ABC):
                                if spanParNode[PARENTA] == '':
                                    spanParNode[PARENTA] = nodeparA
                            if (childEl[PHASE] == PH_B or childEl[PHASE] == PH_AB or childEl[PHASE] == PH_BC or childEl[PHASE] == PH_ABC):
                                if spanParNode[PARENTB] == '':
                                    spanParNode[PARENTB] = nodeparB
                            if (childEl[PHASE] == PH_C or childEl[PHASE] == PH_AC or childEl[PHASE] == PH_BC or childEl[PHASE] == PH_ABC):
                                if spanParNode[PARENTC] == '':
                                    spanParNode[PARENTC] = nodeparC
                            if spanParNode[PARENTA] != '':
                                if spanParNode[PARENTB] != '':
                                    if spanParNode[PARENTC] != '':
                                        spanParNode[PHASE] = PH_ABC
                                    else:
                                        spanParNode[PHASE] = PH_AB
                                else:
                                    if spanParNode[PARENTC] != '':
                                        spanParNode[PHASE] = PH_AC
                                    else:
                                        spanParNode[PHASE] = PH_A
                            else:
                                if spanParNode[PARENTB] != '':
                                    if spanParNode[PARENTC] != '':
                                        spanParNode[PHASE] = PH_BC
                                    else:
                                        spanParNode[PHASE] = PH_B
                                else:
                                    spanParNode[PHASE] = PH_C
                    # Delete the extra node
                    write_log("Extraneous Node. Intending to delete node: " + curEl[NAME])
                    write_log("Extraneous Node. Deleting node: " + elements[outfile_index[get_outfile_index(idx)]][NAME])
                    outfile_index.pop(get_outfile_index(idx))
                # If the node was edited and the child element is a line/span, we 
                # need to make sure there's a node on the other end of the line 
                # because it's a bidirectionally fed line
                elif (childEl[ETYPE] == OHSPAN or childEl[ETYPE] == UGSPAN):
                    childnode, childidx = find_node_sw_ocd(childEl[XL], childEl[YL])
                    if childnode == None:
                        # Node not found, add one
                        write_log("Adding Node " + curEl[NAME] + "_1")
                        newnode = [curEl[NAME] + '_1', '8', childEl[PHASE], childEl[NAME], '', childEl[XL], childEl[YL], 
                            '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                            nodeparA, nodeparB, nodeparC, '1', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                            '{' + str(generatedNodeCount) + '}', childEl[GUID]]
                        elements.append(newnode)
                        outfile_index.insert(get_outfile_index(idx + 2), len(elements) - 1)   
                        generatedNodeCount = generatedNodeCount + 1
                    elif childnode[ETYPE] == SWITCH or childnode[ETYPE] == OCDEV:
                        # Node not found, but switch or overcurrent device found
                        write_log("Found switch or overcurrent device, no need to add node. Found switch or overcurrent device is " + childnode[NAME])
                    else:
                        # Node found, see if we need to add missing phases
                        write_log("Found node, no need to add. Found node is " + childnode[NAME])
                        if childnode[PHASE] < childEl[PHASE]:
                            # Add missing phases
                            childnode[PHASE] = childEl[PHASE]
                            if childnode[PARENTA] == '':
                                write_log("Adding A-Phase parent to " + childnode[NAME] + " of " + nodeparA)
                                childnode[PARENTA] = nodeparA
                            if childnode[PARENTB] == '':
                                write_log("Adding A-Phase parent to " + childnode[NAME] + " of " + nodeparA)
                                childnode[PARENTB] = nodeparB
                            if childnode[PARENTC] == '':
                                write_log("Adding A-Phase parent to " + childnode[NAME] + " of " + nodeparA)
                                childnode[PARENTC] = nodeparC

###############################################################################
# Finds the elements surrounding a switch that may need to be edited. 
# Specifically the switch top, switch bottom, the coincident node, the two
# adjacent lines, and the parents of the two adjacent lines
###############################################################################
def find_switch_parts(swTop):
    matchEl, idx, mid = find_record(elements, x_index, swTop[XL], XL)
    swNode = None
    swBot = None
    swSpanA = None
    swSpanB = None
    nodeSpanA = None
    parSpanB = None
    endSearch = 0
    if matchEl != None:
        # Start with matching coordinates to find as many parts of the switch as possible
        while endSearch == 0 and (float(matchEl[XL]) < float(swTop[XL]) + 0.01) and (float(matchEl[XL]) > float(swTop[XL]) - 0.01):
            write_log("Matching element: " + matchEl[NAME])
            if (float(matchEl[YL]) < float(swTop[YL]) + 0.01) and (float(matchEl[YL]) > float(swTop[YL]) - 0.01):
                if matchEl[ETYPE] == NODE:
                    write_log("Found Node: " + matchEl[NAME])
                    if swNode == None:
                        # Save the node
                        swNode = matchEl
                    else:
                        # Note the extra node for processing, but delete the extra node from the output file
                        write_log("Duplicate node found, deleting " + matchEl[NAME])
                        write_log("Deleting node: " + elements[outfile_index[get_outfile_index(idx)]][NAME])
                        outfile_index.pop(get_outfile_index(idx))
                        swNodeB = matchEl
                elif matchEl[ETYPE] == SWITCH and matchEl[NAME] != swTop[NAME]:
                    write_log("Found switch bottom: " + matchEl[NAME])
                    swBot = matchEl
                elif matchEl[ETYPE] == UGSPAN or matchEl[ETYPE] == OHSPAN:
                    if swSpanB == None:
                        write_log("Found span B: " + matchEl[NAME])
                        swSpanB = matchEl
                    else:
                        write_log("Found span A: " + matchEl[NAME])
                        swSpanA = matchEl
            else:
                endSearch = 1
            mid = mid + 1
            idx = x_index[mid]
            matchEl = elements[idx]
        # Not bidirectional, return values without searching further
        if swNode == None:
            return swBot, swNode, swSpanA, swSpanB, nodeSpanA, parSpanB
        # if the switch bottom wasn't found
        if swBot == None:
            swBot, idx, mid = find_record(elements, name_index, swTop[SWPARTNER], NAME)
            if swBot == None:
                return None, None, None, None, None, None
        # if span B wasn't found
        if swSpanB == None:
            if swNode[PARENTA] != '':
                swSpanB, idx, mid = find_record(elements, name_index, swNode[PARENTA], NAME)
            elif swNode[PARENTB] != '':
                swSpanB, idx, mid = find_record(elements, name_index, swNode[PARENTB], NAME)
            elif swNode[PARENTC] != '':
                swSpanB, idx, mid = find_record(elements, name_index, swNode[PARENTC], NAME)
            if swSpanB == None:
                return None, None, None, None, None, None
        # Now try to find the switch Spans
        if swSpanA == None:
            if swNode[PARENTA] != '' and swNode[PARENTA] != swSpanB[NAME] and swNode[PARENTA] != swTop[NAME] and swNode[PARENTA] != swBot[NAME]:
                swSpanA, idx, mid = find_record(elements, name_index, swNode[PARENTA], NAME)
            elif swNode[PARENTB] != '' and swNode[PARENTB] != swSpanB[NAME] and swNode[PARENTB] != swTop[NAME] and swNode[PARENTB] != swBot[NAME]:
                swSpanA, idx, mid = find_record(elements, name_index, swNode[PARENTB], NAME)
            elif swNode[PARENTC] != '' and swNode[PARENTC] != swSpanB[NAME] and swNode[PARENTC] != swTop[NAME] and swNode[PARENTC] != swBot[NAME]:
                swSpanA, idx, mid = find_record(elements, name_index, swNode[PARENTC], NAME)
            elif swTop[PARENT] != '' and swTop[PARENT] != swSpanB[NAME]:
                swSpanA, idx, mid = find_record(elements, name_index, swTop[PARENT], NAME)
            elif swBot[PARENT] != '' and swBot[PARENT] != swSpanB[NAME]:
                swSpanA, idx, mid = find_record(elements, name_index, swBot[PARENT], NAME)
            elif swNodeB[PARENTA] != '' and swNodeB[PARENTA] != swSpanB[NAME] and swNodeB[PARENTA] != swTop[NAME] and swNodeB[PARENTA] != swBot[NAME]:
                swSpanA, idx, mid = find_record(elements, name_index, swNodeB[PARENTB], NAME)
            elif swNodeB[PARENTB] != '' and swNodeB[PARENTB] != swSpanB[NAME] and swNodeB[PARENTB] != swTop[NAME] and swNodeB[PARENTB] != swBot[NAME]:
                swSpanA, idx, mid = find_record(elements, name_index, swNodeB[PARENTB], NAME)
            elif swNodeB[PARENTC] != '' and swNodeB[PARENTC] != swSpanB[NAME] and swNodeB[PARENTC] != swTop[NAME] and swNodeB[PARENTC] != swBot[NAME]:
                swSpanA, idx, mid = find_record(elements, name_index, swNodeB[PARENTB], NAME)
            # We have spanB load side at the switch, spanA source side at the switch, child of spanA will always be a node
            if swSpanA == None:
                return None, None, None, None, None, None
            write_log("Found SpanA: " + swSpanA[NAME])
            # Find the child of span A-Phase
            nodeSpanA, idx, mid = find_record(elements, x_index, swSpanA[XL], XL)
            if nodeSpanA == None:
                return None, None, None, None, None, None
            write_log(nodeSpanA[NAME])
            while nodeSpanA[ETYPE] != NODE and swSpanA[XL] == nodeSpanA[XL]:
                mid = mid + 1
                idx = x_index[mid]
                nodeSpanA = elements[idx]
                write_log(nodeSpanA[NAME])
            write_log("Found nodeSpanA: " + nodeSpanA[NAME])
            # Find the parent of spanB
            parSpanB, idx, mid = find_record(elements, name_index, swSpanB[PARENT], NAME)
            if parSpanB == None:
                return None, None, None, None, None, None
        else:
            # We have spanB load side at the switch, spanA load side at the switch
            # Find the parent of spanB
            parSpanB, idx, mid = find_record(elements, name_index, swSpanB[PARENT], NAME)
            # Find the parent of spanA
            nodeSpanA, idx, mid = find_record(elements, name_index, swSpanA[PARENT], NAME)
            # Make sure we don't have spanA and spanB swapped
            if parSpanB == None:
                return None, None, None, None, None, None
            if nodeSpanA == None:
                return None, None, None, None, None, None
            if nodeSpanA[ETYPE] != NODE:
                # Swap spanA and spanB
                temp = swSpanB
                swSpanB = swSpanA
                swSpanA = temp
                # Swap nodeSpanA and parSpanB
                temp = parSpanB
                parSpanB = nodeSpanA
                nodeSpanA = temp
        return swBot, swNode, swSpanA, swSpanB, nodeSpanA, parSpanB
    else:
        return None, None, None, None, None, None

###############################################################################
# Fix the open point
###############################################################################
def fix_open_point(swTop, swBot, swNode, swSpanA, swSpanB, nodeSpanA, parSpanB):
    write_log("Bidirectional switch found: " + swTop[NAME])
    write_log("Switch bottom: " + swBot[NAME] + " Switch Node: " + swNode[NAME] + " SpanA: " + swSpanA[NAME] + " SpanB: " + swSpanB[NAME] + " ParA: " + nodeSpanA[NAME] + " ParB: " + parSpanB[NAME])
    # Set the parent of the top of the switch to spanB
    write_log("Setting switch top " + swTop[NAME] + " parent to " + swSpanB[NAME])
    swTop[PARENT] = swSpanB[NAME]
    swTop[PARENTGUID] = swSpanB[GUID]
    # Delete the switch Node
    matchEl, idx, mid = find_record(elements, name_index, swNode[NAME], NAME)
    write_log("Intending to delete node: " + swNode[NAME])
    write_log("Deleting node: " + elements[outfile_index[get_outfile_index(idx)]][NAME])
    outfile_index.pop(get_outfile_index(idx))
    # Set the parent of the bottom of the switch and the switch node based on the direction of A
    if swSpanA[XL] == swNode[XL] and swSpanA[YL] == swNode[YL]:
        # Switch is on the load side of swSpanA
        write_log("Switch is on the load side of SpanA")
        # Switch node parent is spanA
        write_log("Setting switch bottom " + swBot[NAME] + " parent to " + swSpanA[NAME])
        swBot[PARENT] = swSpanA[NAME]
        swBot[PARENTGUID] = swSpanA[GUID]
    else:
        # Switch is on the source side of swSpanA
        write_log("Switch is on the source side of SpanA")
        # Switch bottom is the parent, so the parent is empty
        write_log("Setting switch bottom " + swBot[NAME] + " parent to Empty")
        swBot[PARENT] = ''
        swBot[PARENTGUID] = ''
        # SpanA parent is the swBot
        write_log("Setting spanA " + swSpanA[NAME] + " parent to " + swBot[NAME])
        swSpanA[PARENT] = swBot[NAME]
        swSpanA[PARENTGUID] = swBot[GUID]

###############################################################################
# Processes switches (open points) involved with bidirectionally fed lines
###############################################################################
def process_switches():
    write_log('Processing switches (open points)...')
    # Traverse the elements looking for switches
    for idx in range(len(elements)):
        swTop = elements[idx]
        # If the switch has multiple phases
        if swTop[ETYPE] == SWITCH and swTop[PHASE] > '3':
            processed = 0
            for switch in switches_processed:
                if switch[NAME] == swTop[NAME]:
                    processed = 1
            if processed == 0:
                write_log("Switch found: " + swTop[NAME])
                # Find the other elements of the switch
                swBot, swNode, swSpanA, swSpanB, nodeSpanA, parSpanB = find_switch_parts(swTop)
                # If the switch is bidirectional
                if swNode != None:
                    # Fix the open point
                    fix_open_point(swTop, swBot, swNode, swSpanA, swSpanB, nodeSpanA, parSpanB)
                    # Note that it is fixed
                    switches_processed.append(swBot)
                    switches_processed.append(swTop)
            else:
                write_log("Switch already processed: " + swTop[NAME])

###############################################################################
# Processes switches (open points) involved with bidirectionally fed lines
###############################################################################
def record_openpts():
    openptf = open(openptfile, 'w')
    for row in switches_processed:
        openptf.write(row[NAME] + '\n')
    openptf.close()

###############################################################################
# Processes switches (open points) involved with bidirectionally fed lines
###############################################################################
def record_nodes():
    opennodef = open(nodefile, 'w')
    for idx in range(len(outfile_index)):
        curEl = elements[outfile_index[idx]]
        if curEl[ETYPE] == NODE:
            # Get the phase parents    
            if curEl[PARENTA] != '':
                parA, parAidx, parAmid = find_record(elements, name_index, curEl[PARENTA], NAME)
            else:
                parA = None
            if curEl[PARENTB] != '':
                parB, parAidx, parAmid = find_record(elements, name_index, curEl[PARENTB], NAME)
            else:
                parB = None
            if curEl[PARENTC] != '':
                parC, parAidx, parAmid = find_record(elements, name_index, curEl[PARENTC], NAME)
            else:
                parC = None
            # if the phase parents are not underground line
            if parA != None:
                if parA[ETYPE] != UGSPAN:
                    opennodef.write(curEl[NAME] + ' ' + parA[NAME] + '\n')
            if parB != None:
                if parB[ETYPE] != UGSPAN:
                    opennodef.write(curEl[NAME] + ' ' + parB[NAME] + '\n')
            if parC != None:
                if parC[ETYPE] != UGSPAN:
                    opennodef.write(curEl[NAME] + ' ' + parC[NAME] + '\n')
    opennodef.close()
    
###############################################################################
# M A I N   P R O G R A M
###############################################################################

# Track time
start_time = time.time()

# Open the logfile
logfile = open(logfile, 'w')

# Copy and extract the zip file from the GIS server
if COPYFROMTOGIS == 1:
    inputfile = copy_extract(inputzipfile)

# Read the input STD file
write_log("Reading STD file..." + inputfile)
elements, name_index, x_index, parent_index = read_STD_file(inputfile)

# Create an index for the output STD file before we process anything
outfile_index = range(len(elements))

# It's important to note that now nothing can be inserted or deleted into 
# elements without corrupting the indexes, but records can be appended. 
# When elements are appended the outfile_index must be updated to "insert" 
# the record into the output file.
#
# The code for "inserting" record in the output file at line number idx is
#
#   elements.append(record)
#   outfile_index.insert(get_outfile_index(idx), len(elements) - 1)
#
# Similarly, you can't delete elements directly.
#
# The code for "deleting" record at idx is
#
#   outfile_index.pop(get_outfile_index(idx))
#

# Process the open protection devices
process_overcurrent_devices()

# Process the nodes
process_nodes()

# Process the open points. This should be called after the process_nodes() 
# because it assumes the nodes have already been processed and corrected
process_switches()

# Write the output STD file
write_log("Writing STD file...")
write_array_to_csv(elements, outputfile)

# Copy the fixed STD file to the GIS server
if COPYFROMTOGIS == 1:
    copy_back(outputfile, outputfilefinal)

# Save a list of the open points processed
record_openpts()

# Save a list of nodes to review in the GIS
record_nodes()

# Log the elapsed time
write_log("Elapsed time: %8.2f seconds" % (time.time() - start_time))

# Close the log file
logfile.close()
