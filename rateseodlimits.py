'''
Id: "$Id: rateseodlimits.py,v 1.7 2025/08/14 06:12:02 Exp $"
Description:
Test: qz.remoterisk.tests.unittests.cftc.limits.rateseodlimits
'''
import sandra
import qztable
from qz.core import bobfns
from qz.tools.gov.lib import logging

from qz.data.qztable_utils import tableFromListOfDicts
from qz.remoterisk.utils.bob_utils import getBobEnvironment
from qz.remoterisk.cftc.utils.config import CFTCConfStatic
from qz.remoterisk.cftc.limits.rateseodsnapshots import combineWithEarlierSnapshots
from qz.remoterisk.cftc.limits.utils import jobTimestamp, notifyEODEmptyMeasureExposures, concatenateExpTables,notifyCFTCReportFailure
from qz.remoterisk.cftc.limits.breachcalculator import BreachCalculator
from qz.remoterisk.cftc.limits.rateseoddatasources import dataSourceFactory
from qz.remoterisk.cftc.limits.rateseodalerts import alertEmail
from qz.remoterisk.cftc.configs.limitsconfig import RATESLIMITS
from qz.remoterisk.cftc.utils.persistence import BUS_AREA_COL, DESK_COL, MEASURE_COL, LETIER1_COL, CURRENCY_COL,
EXPOSURES_COL, EXPOSURES_USD_COL, UTILIZATION_COL, SNAPSHOTS, SNAPTIME, writeExposures

logger = logging.getLogger(name)

class RatesEODLimits(BreachCalculator):

def __init__(self, config):
    # config = "uat_rates_eod_yaml_mapping"
    self.config = config
    self.finalExpTable = None
    self.snapshotsDict = {}
    self.jobTimestamp = jobTimestamp()
    self.batchTime = self.timeStamp()
    self.bobEnv = getBobEnvironment()
    self.regionalTimestamp = self.timeStamp(regionalTime=True)  

def fetchLimits(self):
    '''
    to read the limits from limit config file.

    :returns: limit data 
    :rtype: qztable
    '''
    
    limitsTable = tableFromListOfDicts(RATESLIMITS)
    return limitsTable

def combineDiffSourceSnapshots(self, snapshots):
    for key in snapshots.keys():
        if self.totalSnapshots.get(key,None):
            snapshotCols = self.totalSnapshots[key].columnNames()
            expTableCols = snapshots[key].columnNames()
            # to combine data from 2 diff datasources
            if snapshotCols != expTableCols:
                snapshots[key] = snapshots[key].project(snapshotCols)
                
            self.totalSnapshots[key] = self.totalSnapshots[key].vConcat(snapshots[key])
        else:
            self.totalSnapshots[key] = snapshots[key]
    return self.totalSnapshots
    

def determineExposure(self):
    self.yamlConfig = CFTCConfStatic(self.config)
    logger.info(f"config to be used for the utilization calculation - {self.yamlConfig}")
    self.sender = self.yamlConfig['mail']
    self.recipients = self.yamlConfig['recipients_email']
    self.db = self.yamlConfig['exposure_db']
    self.dbPath = sandra.db.join(self.yamlConfig['exposure_path'], self.batchTime.runDate)
    # iterate over vtdNames for given business area
    for name in self.yamlConfig.get('yaml_mapping', {}):
        cfg = self.bobEnv + '_' + self.yamlConfig['yaml_mapping'][name]
        self.cfg = CFTCConfStatic(cfg)
        dataSources = self.cfg['sources']
        dataSourceKeys = list(dataSources.keys())
        self.totalSnapshots = {}
        self.vtdExpTable = None
        for sourceKey in dataSourceKeys:
            snapshotsForSource, expTable, fieldsDict = dataSourceFactory(self.cfg, sourceKey, dataSources, self.jobTimestamp)
            if expTable:
                for key in snapshotsForSource.keys():
                    #TODO update snapshots for all Levels (LOB, VTD)
                    snapshotsForSource[key] = self.removeExposureColumn(snapshotsForSource[key])
                    snapshotsForSource[key] = self.addLegalEntityColumn(snapshotsForSource[key])
                # add combinesnapshots method here to add snapshots from diff datasources
                self.totalSnapshots = self.combineDiffSourceSnapshots(snapshotsForSource)
                
                expTable = self.removeExposureColumn(expTable)
                expTable = self.addLegalEntityColumn(expTable) 
                limitsTable = self.fetchLimits()
                #TODO take level from dataSources instead of yaml file
                calcLevels = fieldsDict.get('calc_level', None)
                
                for calcLevel in calcLevels:
                    calcLevelLimitsTable = limitsTable[limitsTable['Calculation Level'] == calcLevel]
                    colList = self.addCalcLevelCols(calcLevel)
                    # get the expTable at the calc level
                    expTableAtLevel = self.getExpAtCalcLevel(expTable, colList)
                    calcLevelTable = calcLevelLimitsTable.join(expTableAtLevel, colList, mergeKeyCols=True)
                    calcLevelTable = self.shiftCalculation(calcLevelTable)
                    calcLevelTable = calcLevelTable.extend(lambda exp,value: abs(exp/value*100), [EXPOSURES_USD_COL, 'Limit Value'], UTILIZATION_COL, 'double')

                    self.finalExpTable = concatenateExpTables(self.finalExpTable, calcLevelTable)
                    self.vtdExpTable = concatenateExpTables(self.vtdExpTable, calcLevelTable)
                    self.level = self.vtdExpTable['Level'].uniqueRows()[0][0]
        fieldsDict.update({'level': self.level})
        self.utilizationCalculation()
        self.snapshotCreation()
        self.contentsCreation()
        if fieldsDict.get('measuresMissingExposures',None):
            logger.info('Measure are missing for %s',self.level)
            notifyEODEmptyMeasureExposures(fieldsDict,self.regionalTimestamp.runHour,self.cfg)
    
def utilizationCalculation(self):
    '''
    In case of fetching exposures from multiple sources for a single limit code, the below will be used to groupBy data at limitcode level
    '''
    
    cols = list(self.finalExpTable.columnNames())
    cols.remove(EXPOSURES_USD_COL)
    cols.remove(UTILIZATION_COL)
    self.finalExpTable = self.finalExpTable.groupBy(cols,f'sum({EXPOSURES_USD_COL})')
    self.vtdExpTable = self.vtdExpTable.groupBy(cols,f'sum({EXPOSURES_USD_COL})')
    self.finalExpTable = self.finalExpTable.extend(lambda exp,value: abs(exp/value*100), [EXPOSURES_USD_COL, 'Limit Value'], UTILIZATION_COL, 'double')
    self.vtdExpTable = self.vtdExpTable.extend(lambda exp,value: abs(exp/value*100), [EXPOSURES_USD_COL, 'Limit Value'], UTILIZATION_COL, 'double')
    return self.finalExpTable

def snapshotCreation(self):
    currentSnapshots = {SNAPSHOTS:self.totalSnapshots}
    currentSnapshots[SNAPTIME] = self.regionalTimestamp.asDatetime
    combinedSnapshots = combineWithEarlierSnapshots(self.cfg, self.batchTime.sandraRunHour, currentSnapshots, self.level)
    self.snaps = self.getSnapsOrderedByCols(combinedSnapshots[SNAPSHOTS])
    self.snapTime = combinedSnapshots[SNAPTIME]
    
def contentsCreation(self):
    '''
    To create the contents of container to write in to sandra.

    '''
    contents = {EXPOSURES_COL:self.vtdExpTable}
    contents.update({SNAPSHOTS:self.snaps})
    contents[SNAPTIME] = self.snapTime
    dbExpPath = sandra.db.join(self.dbPath, self.level)
    self.snapshotsDict.update({self.level:contents[SNAPSHOTS]})
    writeExposures(self.db, dbExpPath, contents, self.batchTime.sandraRunHour)

def getExpAtCalcLevel(self, expTable, colList):
    '''
    for the calculation and get final exposure table for that level

    :param qztable expTable: exposure
    :param list colList: column list
    :returns: final exposure table for the calculation level 
    :rtype: qztable
    '''
    
    # do the calculation and get final exposure table for that level
    expTableAtCalcLevel = expTable.groupBy(colList, 'sum(Exposures_USD)')
    
    if DESK_COL in colList:
        expTableAtCalcLevel.renameCol(DESK_COL, 'Level')
        colList.remove(DESK_COL)
    else:
        expTableAtCalcLevel.renameCol(BUS_AREA_COL, 'Level')
        colList.remove(BUS_AREA_COL)
    colList.append('Level')
    return expTableAtCalcLevel

def addCalcLevelCols(self, level):
    '''
    To create column list according to the utilization calculation level.

    :param strin level: calculation level
    :returns: column list 
    :rtype: list
    '''
    if level == 'LE':
        colList = [BUS_AREA_COL, LETIER1_COL, MEASURE_COL]
    elif level == 'Currency':
        colList = [BUS_AREA_COL, CURRENCY_COL, MEASURE_COL]
    elif level == 'VTD':
        colList = [DESK_COL, MEASURE_COL]
    elif level == 'VTD+Currency':
        colList = [DESK_COL, CURRENCY_COL, MEASURE_COL]
        
    return colList

def notifyEmail(self):
    '''
    To send limit utilization email to reciepients with snapshot attachement.
    '''
    date = self.regionalTimestamp.cobDate
    snapTimeVal = self.regionalTimestamp.snapTime
    tzAbbrForSub = self.regionalTimestamp.tzAbbr
    alertEmail(self.sender, self.recipients, date, snapTimeVal, tzAbbrForSub, self.finalExpTable, self.snapshotsDict)
    
def shiftCalculation(self, expTable):
    '''
    Calculate 10% shock for M10 and P10 IR Vega limits and project on RESULTING_COLS.
    '''
    RESULTING_COLS = ['Level', 'Limit Name','LETier1', 'Measure', 'Limit Value', 'Exposures_USD']
    vegaExpTable = expTable[expTable.Measure=='IR Vega' and (expTable['Limit Name'].contains('M10%')| expTable['Limit Name'].contains('P10%'))]
    negationVegaExpTable = expTable[~(expTable.Measure=='IR Vega' and (expTable['Limit Name'].contains('M10%')| expTable['Limit Name'].contains('P10%')))]\
                            .project(RESULTING_COLS)
    
    vegaMulExpTable = vegaExpTable.extendExprs([f'{EXPOSURES_USD_COL}*Shift_Name'],['Exposures_Vega'], ['double'])\
                    .project([EXPOSURES_USD_COL], exclude=True)\
                    .rename(['Exposures_Vega'],[EXPOSURES_USD_COL])\
                    .project(RESULTING_COLS)
    
    expTable = qztable.vConcat([vegaMulExpTable,negationVegaExpTable])
    return expTable
def run(config='dev_rates_eod_yaml_mapping'):
'''
Entry point to store limit data.
:param str config: yaml config name
'''    
try:
    #raise RuntimeError('Test Exception')
    cfg = CFTCConfStatic(config)
    obj = RatesEODLimits(config)
    obj.determineExposure()    
    obj.notifyEmail()
except Exception as e:
    notifyCFTCReportFailure(
        reportName   = 'CFTC EOD limit based check report',
        exception    = e,
        toRecipients = [cfg.get('ficc_risk_support_mail',cfg['mail'])],
        ccRecipients = cfg['recipients_email']+[cfg['mail']],
        sender       = cfg['mail']
        )
    raise
def main():
logging.compliance(name, "Bob Run", action=logging.Action.ENTRYPOINT)
bobfns.run(run)
