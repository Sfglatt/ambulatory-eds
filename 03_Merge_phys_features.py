import csv  # Use for CSV processing, reading, and writing
import datetime  # Used to handle all Datetimes used in files
import os  # Used to get all files in directory
import math  # Used for the floor function, for finding the nearest period to the current time

DESIRED_FREQUENCY = 4  # The frequency that all the files will be merged into
# Every time desired frequency is measured, this is the file being referred to
# A value of 4 is 4Hz, so the period is 1/4 of a second or 0.25s

class FeatureType:  # Class that defines each feature
    def __init__(self, name, timeIndex, headers, frequency, mergeOrder) -> None:
        self.name = name
        self.timeIndex = timeIndex
        self.dataIndexes = []
        self.headers = headers
        self.frequency = frequency
        self.period = 1 / frequency
        self.mergeOrder = mergeOrder

FEATURES = [  # List of all possible feature types and their attributes
    FeatureType(name="HR", timeIndex=1, headers=["HR"], frequency=1, mergeOrder=1),
    FeatureType(name="ACC", timeIndex=1, headers=["X", "Y", "Z"], frequency=32, mergeOrder=2),
    FeatureType(name="TEMP", timeIndex=1, headers=["TEMP"], frequency=4, mergeOrder=3),
    FeatureType(name="BVP", timeIndex=1, headers=["BVP"], frequency=64, mergeOrder=4),
    FeatureType(name="EDA", timeIndex=1, headers=["EDA", "event", "code"], frequency=4, mergeOrder=5)
]

# List of feature names, used for logging and for file detection
FEATURENAMES = list(map(lambda i: i.name, FEATURES)) 

def sortByTime(filePath):  # Function that sorts the unmerged files by time
    # With the file path, it opens the file and sets it up to be read
    readObj = open(filePath, "r")
    csvReader = csv.reader(readObj)

    # Sets the header to the first line of the file
    header = next(csvReader)

    # All rows are read and added to the rows list
    rows = []
    for row in csvReader:
        rows.append(row)

    readObj.close()  # Read object is closed

    # Obtains the index of the time column and sorts by the time
    index = header.index("timestamp")
    rows = sorted(rows, key=lambda i: datetime.datetime.fromisoformat(i[index]))

    # The file is opened again to write in it the sorted lines
    writeObj = open(filePath, "w", newline="")
    csvWriter = csv.writer(writeObj)

    # First the header row is added, then the sorted lines are added
    csvWriter.writerow(header)
    csvWriter.writerows(rows)

    writeObj.close()  # Then the file is closed again

# Function to turn a string with the time into a Datetime object
def getTime(timeString) -> datetime:
    return datetime.datetime.fromisoformat(timeString)

# Function is used to find the nearest period, depending on the desired frequency 
def findNearestPeriod(date):
    return datetime.datetime.fromtimestamp(math.floor(date.timestamp() * DESIRED_FREQUENCY) / DESIRED_FREQUENCY) 

# Class that takes in a line and processes the desired columns
class lineProcessor:
    def __init__(self, line, timeIndex, dataIndexes):
        self.time = getTime(line[timeIndex])
        self.data = []
        for dataIndex in dataIndexes:  # For-loop through the desired data indices
            # Captures the column's information into the list of desired data
            self.data.append(line[dataIndex])  

# Class that takes in a file path and processes each column at the desired intervals
class fileProcessor:
    # Constructor takes in a file path and feature type of the file
    def __init__(self, filePath, type) -> None:
        self._filePath = filePath
        self._setFeature(type)
        self._openfile()
        self._setLength()
        self._setHeaders()

    # Sets the feature attribute of the file process's type (Ex. ACC, EDA, ...)
    def _setFeature(self, type):
        featureType = FEATURES[FEATURENAMES.index(type)]

        # Sets the feature and attribute with the unique information of the file's type
        try:
            self.feature = FeatureType(featureType.name, featureType.timeIndex, featureType.headers, featureType.frequency, featureType.mergeOrder)
        except:
            return TypeError

    # Function to open the file and create the reader to read each line
    def _openfile(self):
        self._readObj = open(self._filePath, "r")
        self._reader = csv.reader(self._readObj)

    # Saves the length of the file, so that the reader doesn't go past the max size
    def _setLength(self):
        self.size = len(self._readObj.readlines())
        self.closeFile()
        self._openfile()
    
    # Saves the first line of the file as the header, to know where the information is
    def _setHeaders(self):
        headerLine = next(self._reader)
        self.currentLine = 1
        self.headers = []

        # For-loop through the header, to save the desired ones
        for header in self.feature.headers:
            if header in headerLine and headerLine.index(header) not in self.feature.dataIndexes:
                self.feature.dataIndexes.append(headerLine.index(header))
                self.headers.append(header)
            else:
                continue
            
    # Function to get the next line of file and save the line's information
    def nextLine(self):
        self.currentLine += 1
        if self.currentLine >= self.size:
            self.lastLine = None
        else:
            nextLine = next(self._reader)
            self.lastLine = lineProcessor(nextLine, self.feature.timeIndex, self.feature.dataIndexes)
        
        return self.lastLine

    def nextPeriod(self):  # Returns the next line that corresponds to the next quarter-second period
        try:
            self.nextLine()
            # While-loop through the following lines in the file to find the nearest row
            # that corresponds to the next desired period 
            while self.lastLine.time != findNearestPeriod(self.lastLine.time):
                self.nextLine()
        except AttributeError:
            return self.lastLine

        return self.lastLine
    
    # Function to do the necessary processing to close and delete the file process
    def __del__(self):
        self._readObj.close()  # Closes the read object

    # Function to manually close the file processor, as the delete function 
    def closeFile(self):
        self._readObj.close()  # Closes the read object

# Function that takes in all feature files and the name of the merge file
def MergeFiles(files, newFilePath):
    global DEBUG
    # Opens a new file, 1 for merge, and another for the debug (debug has more information)
    mergeFile = open(newFilePath + "MERGED.csv", "w", newline="")
    debugFile = open(newFilePath + "DEBUG.csv", "w", newline="")

    # Opens a write object for both files
    mergeWriter = csv.writer(mergeFile)
    debugWriter = csv.writer(debugFile)

    # Writes the first two columns of the merge header
    mergedHeader = ["timer", "timestamp"]

    # For-loop through all file's headers and adds it to merge files header
    for file in files:
        for header in file.headers:
            mergedHeader.append(header)
            
    mergeWriter.writerow(mergedHeader)  # Writes the merge header to the merge file

    debugHeader = mergedHeader.copy()  # Makes a copy of the merge header for debug header

    # For-loop through each feature and adds a column for each file's line where
    # the information was taken from, for debug purposes
    for feature in sorted(FEATURES, key=lambda i: i.mergeOrder):
        debugHeader.append(feature.name + " line #")

    debugWriter.writerow(debugHeader)  # Writes the debug header to the debug file

    [curFile.nextPeriod() for curFile in files]  # Gets the next desired period for each file

    lineCount = 1  # Keep track of the merge and debug file's line count
    timer = 0  # The timer that is kept for the merge and debug file
    previousTimestamp = None  # Previous timestamp, used for keeping track of the timer column

    # While-loop as long as the number of files left to merge is greater than 0
    while len(files) > 0:
        # Current timestamp is the earliest timestamp of the current line of all files
        currentTimestamp = sorted(files, key=lambda i: i.lastLine.time)[0].lastLine.time

        # Conditional statement that checks if the current timestamp is in a new 10-minute period
        if previousTimestamp is not None and (currentTimestamp - previousTimestamp).total_seconds() > 0.25:
            timer = 0
        else:
            timer += 0.25
        
        # Starts the current line with the first 2 essential columns for the merge and debug file
        currentLine = [timer, currentTimestamp]

        # Line numbers are kept for debug purposes, to keep track of where the information was obtained from 
        lineNumbers = []

        # For-loop through each file that is still being merged
        for curFile in files:
            # Checks if the curFile's current line's time is equal to the merge file's current timestamp
            if (curFile.lastLine.time - currentTimestamp).total_seconds() < curFile.feature.period:
                # Appends all desired data to the current line of the merge process
                for data in curFile.lastLine.data:
                    currentLine.append(data)
                
                # Adds the line number of the curFile
                lineNumbers.append(curFile.currentLine)
                
                # If the file is at the currentTimestamp, then the next period is obtained
                # This is so that the next time the file is checked, it will be at the next
                # desired period, which will be the next currentTimestamp of the merge process
                if curFile.lastLine.time == currentTimestamp:
                    curFile.nextPeriod()
            else:
                # If the timestamps don't line up with the curFile, then it will add a placeholder
                for data in curFile.lastLine.data:
                    currentLine.append("-")

        # Debug line gets a copy of the current line in the merge process, and line numbers are added
        debugLine = currentLine.copy() + lineNumbers
                
        # Timestamps are saved as the previous timestamp, to know if the 10-minute period is over 
        previousTimestamp = currentTimestamp

        # Removes all files whose last line was not found (meaning the file has reached the end)
        files = list(filter(lambda i: i.lastLine is not None, files))

        # If the placeholder is used, then the line is skipped and not added to the merge process
        if "-" in currentLine or len(currentLine) != len(mergedHeader):
            timer -= 0.25
            continue
        
        # Writes the currentLine and debugLine to the respective files
        mergeWriter.writerow(currentLine)
        debugWriter.writerow(debugLine)

        # Increments the lineCount, for logging purposes to know how many lines the
        # merge process has when it is recorded in the output 
        lineCount += 1
        # print(currentLine)

    # Logs that the files are done being merged, and both files are closed after
    print("\nMerged all files into:", newFilePath, "lines:", lineCount)
    mergeFile.close()
    debugFile.close()

# Gets all file names in the current directory 
input = os.path.dirname(__file__)
folderPath = input + "\\"
fileNames = os.listdir(folderPath)

# Dictionary that keeps the participant name, to know which files go with it
participantFiles = {}
mergedNames = []  # Name of all the participants whose names appear in the merge files

# For-loop through all files in the directory
for name in fileNames:
    # For-loop through each of the FEATURENAMES to see if the name is inside the file name
    for featureName in FEATURENAMES:
        if featureName in name and name.find(featureName) != -1:
            # If a feature name is found inside the file name
            mergedName = name[:name.find(featureName)]  # Participant name is added to the names list
            if mergedName not in participantFiles:
                # If it's a new participant, it will create a new list to hold all files for that participant
                participantFiles[mergedName] = []
            
            sortByTime(folderPath + name)  # Sorts the file by time
            file = fileProcessor(folderPath + name, featureName)  # File processor for that file is initiated
            # Logs that the file has been found, with the length
            print("Found", featureName, "File, Name: \"" + name + "\", Size:", file.size)
            participantFiles[mergedName].append(file)  # Adds the file to that participant's list
            break

# For-loop through each participant
for participant in participantFiles:
    # Sorts all participant files by merge order,1:HR 2:ACC 3:EDA ...
    files = sorted(participantFiles[participant], key=lambda i: i.feature.mergeOrder)
    # Merges all files into a new one for that participant
    MergeFiles(files, folderPath + participant)
