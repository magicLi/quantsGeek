#-*- coding=utf-8 -*-
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
import datetime
import time
from xlrd import *
import statsmodels.api as sm
from quantsGeek import * 
import numpy as np
import ConfigParser
import xlwt
import time
import pymat2 as pymat
import socket
import pymssql 
import WindPy as wind
import pickle

class multiFactors():  
    def __init__(self):
        #因子准备
        config = ConfigParser.ConfigParser()
        config.readfp(open('data.ini'))
        self.factorsStr=[config.get("factor","riskFactor"),config.get("factor","fiscalFactors"),\
                      config.get("factor","valueFactors"),config.get("factor","MVFactors")]
        self.windowYear=int(config.get("time","windowYear"))
        self.oosWindowM=int(config.get("time","oosWindowM"))
        self.holdingPeriod=int(config.get("time","holdingPeriod"))
        self.frequency=config.get("time","frequency")
        self.databaseStatus=config.get("database","databaseStatus")
        self.databaseName=config.get("database","databaseName")
        self.indexCode=config.get("index","indexCode") 
        self.benchmark=config.get("index","benchmark")
        self.scorePercent=float(config.get("filter","scorePercent"))
        self.frequency=config.get("time","frequency")
        self.path="%s\outcomePicture\\" %(os.getcwd())
        self.factors=[]
        wind.w.start()
        self.wind=wind
        matlab=pymat.Matlab()
        matlab.start()
        self.matlab=matlab
        
    def backtestCal(self,backtestYear):  
        selectedStockCode={}
        selectedStockWeight={}
        selectedStockStepCode={}
        selectedStockStepWeight={}     
        portMulti={}
        portStep={}
        benchmark={} 
        multiFactors={}
        
        startDate='%s-01-01'%(str((int(backtestYear[0])-self.windowYear)))
        endDate='%s-12-31'%(backtestYear[-1])    
        #提取回溯的全部时间戳
        allDateStr=dataOperation().getDateStrWind(startDate,endDate,self.frequency,self.wind)
        #提取历史因子并处理成为标准化因子
        multiFactors=dataOperation().historicFactorsRetrieve(self.databaseName,self.databaseStatus,multiFactors,self.factorsStr,self.indexCode,self.wind,allDateStr)
                
        for i in range(len(backtestYear)):
            portReturn=[]
            portReturnStep=[]
            startDate='%s-01-01'%(str((int(backtestYear[i])-self.windowYear)))
            endDate='%s-12-31'%(backtestYear[i])
            dateStr=dataOperation().getDateStrWind(startDate,endDate,self.frequency,self.wind)
            [benchmarkReturn,dailyDate]=dataOperation().getDailyReturn(self.benchmark,dateStr[-13],dateStr[-1],self.wind)
            indexCode=dataOperation().getDailyReturn(self.indexCode,dateStr[-13],dateStr[-1],self.wind)[0]
            #每次月进行回测时,往前推进周期为self.oosWindowM个月(总Out of sample周期)
            #,基于前面self.windowYear(回溯周期表现,决定后面self.holdingPeriod(持仓周期)的持仓配置情况,所以月回测,循环self.oosWindowM/self.holdingPeriod次
            for j in range(self.oosWindowM/self.holdingPeriod): 
                #得到持仓周期点换仓时需要获取的回溯周期(月)的时间戳
                btDateStr=dateStr[j*self.holdingPeriod:j*self.holdingPeriod+self.windowYear*12]
                #计算每持仓周期时候需要使用的(回溯周期窗口如2年*12)的因子系数,用回溯周期计算的因子系数平均得出理论因子系数,因此每持仓周期时是跑(回溯周期月)次循环,总共跑self.oosWindowM/self.holdingPeriod次
                #打印测试周期
                print 'TestDate Period is: %s'%([btDateStr[-1],dateStr[(j+1)*self.holdingPeriod+self.windowYear*12-1]])             
                #计算betaMean系数
                [betaMean,betaMeanStep,standadizedFactorsList]=Barra().multiRegressWind(btDateStr,multiFactors,self.matlab,self.indexCode,self.wind)
                #全行业打分选股，市场中性构成组合，比较业绩,multiRegression方法
                selectedStockCode[btDateStr[-1]]=Barra().scoreWind(btDateStr,betaMean,self.scorePercent,standadizedFactorsList,self.indexCode,self.wind) 
                print 'numbers of stocks by multiFactors before weighting is %s:'%(len(selectedStockCode[btDateStr[-1]]))
                selectedStockWeight[btDateStr[-1]]=Barra().getIndustryNeutralWeight(btDateStr[-1],self.benchmark,selectedStockCode[btDateStr[-1]],self.wind)  
                print 'numbers of stocks by multiFactors after weighting is %s:'%(len(selectedStockWeight[btDateStr[-1]]))
                portReturn+=dataOperation().portReturn(selectedStockWeight[btDateStr[-1]],btDateStr[-1],dateStr[(j+1)*self.holdingPeriod+self.windowYear*12-1],self.wind)[1:]
                #全行业打分选股，市场中性构成组合，比较业绩,stepwiseRegression方法  
                selectedStockStepCode[btDateStr[-1]]=Barra().scoreWind(btDateStr,betaMeanStep,self.scorePercent,standadizedFactorsList,self.indexCode,self.wind)     
                print 'numbers of stocks by stepWise before weighting is %s:'%(len(selectedStockStepCode[btDateStr[-1]]))
                selectedStockStepWeight[btDateStr[-1]]=Barra().getIndustryNeutralWeight(btDateStr[-1],self.benchmark,selectedStockStepCode[btDateStr[-1]],self.wind)  
                print 'numbers of stocks by stepWise after weighting is %s:'%(len(selectedStockStepWeight[btDateStr[-1]]))
                portReturnStep+=dataOperation().portReturn(selectedStockStepWeight[btDateStr[-1]],btDateStr[-1],dateStr[(j+1)*self.holdingPeriod+self.windowYear*12-1],self.wind)[1:]
  
            #计算投资业绩
            portMulti[backtestYear[i]]=dataOperation().backtestAttribution(portReturn,self.matlab)
            portStep[backtestYear[i]]=dataOperation().backtestAttribution(portReturnStep,self.matlab)
            benchmark[backtestYear[i]]=dataOperation().backtestAttribution(benchmarkReturn,self.matlab)
            
            #逐年画图
            cumPR=list(np.cumsum(portReturn))
            cumPR1=list(np.cumsum(portReturnStep))
            cumBR=list(np.cumsum(benchmarkReturn))
            cumIR=list(np.cumsum(indexCode))
            absoluteR=[(x-y)*1.00 for x,y in zip(cumPR,cumBR)]
            absoluteR1=[(x-y)*1.00 for x,y in zip(cumPR1,cumBR)]
            #相对收益图
            dataOperation().drawBacktest(cumIR,cumBR,cumPR,cumPR1,dailyDate,self.path,backtestYear[i])
            #绝对收益图        
            dataOperation().drawAbsRBacktest(absoluteR,absoluteR1,dailyDate,self.path,backtestYear[i])

        return portMulti,portStep,benchmark
            
backtestYear=['2010','2011','2012','2013']

if __name__ == "__main__": 
    t0=time.time()
    [portMulti,portStep,benchmark]=multiFactors().backtestCal(backtestYear)
    print 'finish'
    print time.time()-t0