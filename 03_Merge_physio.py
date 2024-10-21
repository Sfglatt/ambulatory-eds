import csv  
import datetime  
import os 
import math  

DESIRED_FREQUENCY = 4  
# A value of 4 is 4Hz, so the period is 1/4 of a second or 0.25s

class FeatureType:  
    def __init__(self, name, timeIndex, headers, frequency, mergeOrder) -> None:
        self.name = name
        self.timeIndex = timeIndex
        self.dataIndexes = []
        self.headers = headers
        self.frequency = frequency
        self.period = 1 / frequency
        self.mergeOrder = mergeOrder

FEATURES = [  # phys feature types 
    FeatureType(name="HR", timeIndex=1, headers=["HR"], frequency=1, mergeOrder=1),
    FeatureType(name="ACC", timeIndex=1, headers=["X", "Y", "Z"], frequency=32, mergeOrder=2),
    FeatureType(name="TEMP", timeIndex=1, headers=["TEMP"], frequency=4, mergeOrder=3),
    FeatureType(name="BVP", timeIndex=1, headers=["BVP"], frequency=64, mergeOrder=4),
    FeatureType(name="EDA", timeIndex=1, headers=["EDA", "event", "code"], frequency=4, mergeOrder=5)
]

FEATURENAMES = list(map(lambda i: i.name, FEATURES)) 

def sortByTime(filePath):  
    readObj = open(filePath, "r")
    csvReader = csv.reader(readObj)

    header = next(csvReader)

    rows = []
    for row in csvReader:
        rows.append(row)

    readObj.close()  

    index = header.index("timestamp")
    rows = sorted(rows, key=lambda i: datetime.datetime.fromisoformat(i[index]))

    writeObj = open(filePath, "w", newline="")
    csvWriter = csv.writer(writeObj)

    csvWriter.writerow(header)
    csvWriter.writerows(rows)

    writeObj.close()  

def getTime(timeString) -> datetime:
    return datetime.datetime.fromisoformat(timeString)

def findNearestPeriod(date):
    return datetime.datetime.fromtimestamp(math.floor(date.timestamp() * DESIRED_FREQUENCY) / DESIRED_FREQUENCY) 

class lineProcessor:
    def __init__(self, line, timeIndex, dataIndexes):
        self.time = getTime(line[timeIndex])
        self.data = []
        for dataIndex in dataIndexes:  
            self.data.append(line[dataIndex])  

class fileProcessor:

    def __init__(self, filePath, type) -> None:
        self._filePath = filePath
        self._setFeature(type)
        self._openfile()
        self._setLength()
        self._setHeaders()

    def _setFeature(self, type):
        featureType = FEATURES[FEATURENAMES.index(type)]

        try:
            self.feature = FeatureType(featureType.name, featureType.timeIndex, featureType.headers, featureType.frequency, featureType.mergeOrder)
        except:
            return TypeError

    def _openfile(self):
        self._readObj = open(self._filePath, "r")
        self._reader = csv.reader(self._readObj)

    def _setLength(self):
        self.size = len(self._readObj.readlines())
        self.closeFile()
        self._openfile()
    
    def _setHeaders(self):
        headerLine = next(self._reader)
        self.currentLine = 1
        self.headers = []

        for header in self.feature.headers:
            if header in headerLine and headerLine.index(header) not in self.feature.dataIndexes:
                self.feature.dataIndexes.append(headerLine.index(header))
                self.headers.append(header)
            else:
                continue
            
    def nextLine(self):
        self.currentLine += 1
        if self.currentLine >= self.size:
            self.lastLine = None
        else:
            nextLine = next(self._reader)
            self.lastLine = lineProcessor(nextLine, self.feature.timeIndex, self.feature.dataIndexes)
        
        return self.lastLine

    def nextPeriod(self):  
        try:
            self.nextLine()
            while self.lastLine.time != findNearestPeriod(self.lastLine.time):
                self.nextLine()
        except AttributeError:
            return self.lastLine

        return self.lastLine
    
    def __del__(self):
        self._readObj.close()  

    def closeFile(self):
        self._readObj.close() 

def MergeFiles(files, newFilePath):
    global DEBUG

    mergeFile = open(newFilePath + "MERGED.csv", "w", newline="")
    debugFile = open(newFilePath + "DEBUG.csv", "w", newline="")

    mergeWriter = csv.writer(mergeFile)
    debugWriter = csv.writer(debugFile)

    mergedHeader = ["timer", "timestamp"]

    for file in files:
        for header in file.headers:
            mergedHeader.append(header)
            
    mergeWriter.writerow(mergedHeader)  
    debugHeader = mergedHeader.copy()  

    for feature in sorted(FEATURES, key=lambda i: i.mergeOrder):
        debugHeader.append(feature.name + " line #")

    debugWriter.writerow(debugHeader)  

    [curFile.nextPeriod() for curFile in files]  
    lineCount = 1  
    timer = 0  
    previousTimestamp = None  

    while len(files) > 0:
        currentTimestamp = sorted(files, key=lambda i: i.lastLine.time)[0].lastLine.time

        if previousTimestamp is not None and (currentTimestamp - previousTimestamp).total_seconds() > 0.25:
            timer = 0
        else:
            timer += 0.25
        
        currentLine = [timer, currentTimestamp]

        lineNumbers = []

        for curFile in files:
            if (curFile.lastLine.time - currentTimestamp).total_seconds() < curFile.feature.period:
                for data in curFile.lastLine.data:
                    currentLine.append(data)
                
                lineNumbers.append(curFile.currentLine)
                
                if curFile.lastLine.time == currentTimestamp:
                    curFile.nextPeriod()
            else:
                for data in curFile.lastLine.data:
                    currentLine.append("-")

        debugLine = currentLine.copy() + lineNumbers
                
        previousTimestamp = currentTimestamp

        files = list(filter(lambda i: i.lastLine is not None, files))

        if "-" in currentLine or len(currentLine) != len(mergedHeader):
            timer -= 0.25
            continue
        
        mergeWriter.writerow(currentLine)
        debugWriter.writerow(debugLine)

        lineCount += 1

    print("\nMerged all files into:", newFilePath, "lines:", lineCount)
    mergeFile.close()
    debugFile.close()

input = os.path.dirname(__file__)
folderPath = input + "\\"
fileNames = os.listdir(folderPath)

participantFiles = {}
mergedNames = []  

for name in fileNames:
    for featureName in FEATURENAMES:
        if featureName in name and name.find(featureName) != -1:
            mergedName = name[:name.find(featureName)] 
            if mergedName not in participantFiles:
                participantFiles[mergedName] = []
            
            sortByTime(folderPath + name)  
            file = fileProcessor(folderPath + name, featureName)  
            print("Found", featureName, "File, Name: \"" + name + "\", Size:", file.size)
            participantFiles[mergedName].append(file) 
            break

for participant in participantFiles:
    files = sorted(participantFiles[participant], key=lambda i: i.feature.mergeOrder)
    MergeFiles(files, folderPath + participant)