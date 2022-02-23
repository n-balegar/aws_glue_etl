from sys import argv
import configparser
config = configparser.ConfigParser()
import logging
#read the parm file name
#adding the parm details to below dictionary
parDetailsDict={}
globalParmDict={}
parmList=[]
globalParmlist=[]
#jobName=argv[1] #read the job name from the argv
#function to get the parm details
def getParmDetails(jobName,parmFileName):
    print("inside the parmfile parser",jobName,parmFileName)
    config.read(parmFileName)
    if jobName not in config.sections():
        print(f"Job Name {jobName} Doesn't exists exiting the script")
        exit(1)
    #getting the parm details for the respective job
    for key in config[jobName]:
       # print(config[jobName][key])
        parms=key+'='+config[jobName][key]
        parDetailsDict[key]=config[jobName][key]
        parmList.append(parms)
    #getting the glbalParamter details
    for key in config['global']:
        parms=key+'='+config['global'][key]
        parDetailsDict[key]=config['global'][key]
        globalParmlist.append(parms)
    #print(parDetailsDict.items(),globalParmDict.items())
    parmList.extend(globalParmlist)
    return parDetailsDict
#getParmDetails(jobName)