#-*- coding=utf-8 -*-
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
import datetime
import time
from xlrd import *
import statsmodels.api as sm
from alpha import * 
import numpy as np
import ConfigParser
import xlwt
import time
import pymat2 as pymat
import socket
import pymssql 

class multiFactors():
    
    def __init__(self):
        #公共参数准备
        config = ConfigParser.ConfigParser()
        config.readfp(open('data.ini'))
        transRateS=float(config.get("Assumption","transRateStock"))
        self.user=config.get('databaseConfig','user')
        self.password=config.get('databaseConfig','password')
        self.selectedFactors=config.get('factor','selectedFactors')
        self.windowYear=2
        self.riskWindow=1    
        self.oosWindowM=12
        self.database='gilData'
        self.localDatabase='multiFactors'
        self.complementaryDatabase='advancedDB'
        self.frequency='M' #Monthly,Weekly,Daily
        self.indexCode='000905' 
        self.benchmark='000300'   
        self.scorePercent=0.4
        
       
    def backtestPrepare(self,startDate,endDate):  
        matlab=pymat.Matlab()
        matlab.start()            
        #利用wind的接口提取日期
        btDateStr=dataOperation().getDateStrWind(startDate,endDate,self.frequency)
        #利用数据库提取日期
        #btDateStr=dataOperation().getDateStr(btStartDate,btEndDate,self.frequency,self.database,self.user,self.password)
        
        #构建因子库,此程序不需要每次都跑
        #利用数据库进行操作
        dataOperation().factorsOperations(btDateStr,self.frequency,self.database,self.localDatabase,self.user,self.password,self.indexCode,self.benchmark,self.windowYear,self.riskWindow,matlab)
        dataOperation().factorsOperationsWind(btDateStr,self.indexCode)
        
    def backtestCal(self,backtestYear):  
        t0=time.time()
        matlab=pymat.Matlab()
        matlab.start()  
        indexCode=str(int(self.indexCode))
        benchmark=str(int(self.benchmark))
        
        selectedStockCode={}
        selectedStockWeight={}
  
           
        for i in range(len(backtestYear)):
            print backtestYear[i]
            #每测试一年,涉及的遍历月份有36个月
            startDate='%s-01-01'%(str((int(backtestYear[i])-self.windowYear)))
            endDate='%s-12-31'%(backtestYear[i])
            #dateStr=dataOperation().getDateStr(startDate,endDate,self.frequency,self.database,self.user,self.password) 
            dateStr=dataOperation().getDateStrWind(startDate,endDate,self.frequency)
            #每次月进行回测时,往前推进周期为12个月,基于前面24个月表现,决定后面一个月的持仓配置情况,所以月回测,循环12次
            for j in range(self.oosWindowM):    
                print j
            #得到每月持仓时需要的24个月的时间戳
                btDateStr=dateStr[j:j+self.windowYear*self.oosWindowM]
            #计算每月(24个月的窗口)的因子系数,用24次计算的因子系数平均得出理论因子系数,因此每个月是跑24次循环,总共跑12个月
            #基于wind接口的betaMean
                [betaMean,standadizedFactorsList]=Barra().multiRegressWind(btDateStr,matlab,self.indexCode)
              #下面是基于数据库的Beta 
                #betaMean=Barra().multiRegress(btDateStr,matlab,indexCode,benchmark,self.localDatabase,self.user,self.password,self.selectedFactors)
            #打分选股，市场中性构成组合，比较业绩
                selectedStockCode[btDateStr[-1]]=Barra().scoreWind(btDateStr,betaMean,self.scorePercent,standadizedFactorsList,self.indexCode)
                #selectedStockCode[btDateStr[-1]]=Barra().score(btDateStr,betaMean,self.scorePercent,self.indexCode,self.benchmark,self.database,self.localDatabase, self.user, self.password,self.selectedFactors)
                selectedStockWeight[btDateStr[-1]]=Barra().getIndustryNeutralWeight(btDateStr[-1],self.benchmark,selectedStockCode[btDateStr[-1]])
            print selectedStockCode
            
            

backtestYear=['2009','2010','2011','2012','2013']

if __name__ == "__main__": 
    startDate='2007-01-01'
    endDate='2013-12-31'
    #multiFactors().backtestPrepare(startDate,endDate)
    multiFactors().backtestCal(backtestYear)