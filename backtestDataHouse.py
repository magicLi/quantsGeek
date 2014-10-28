#-*- coding=utf-8 -*-
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
import datetime
import time
from xlrd import *
#import statsmodels.api as sm
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
        self.windowYear=2  
        self.oosWindowM=12
        self.indexCode='000905' 
        self.benchmark='000300'   
        self.scorePercent=0.4
        self.frequency='M'
        self.path="%s\\" %(os.getcwd())
        self.factors=[]
       
    def backtestPrepare(self,startDate,endDate):  
        matlab=pymat.Matlab()
        matlab.start()            
        #利用wind的接口提取日期
        btDateStr=dataOperation().getDateStrWind(startDate,endDate,self.frequency)
        #构建因子库,此程序不需要每次都跑
        dataOperation().factorsOperationsWind(btDateStr,self.indexCode)
        
    def backtestCal(self,backtestYear):  
        matlab=pymat.Matlab()
        matlab.start()  
        matlab.eval("DH('pabxmlab02001','888888')")
        selectedStockCode={}
        selectedStockWeight={}
        backtestRes={}
        for i in range(len(backtestYear)):
            portReturn=[]
            #每测试一年,涉及的遍历月份有36个月
            startDate='%s-01-01'%(str((int(backtestYear[i])-self.windowYear)))
            endDate='%s-12-31'%(backtestYear[i])
            dateStr=dataOperation().getDateStrDHM(startDate,endDate,matlab)            
            [benchmarkReturn,dailyDate]=dataOperation().getDailyReturn(self.benchmark,dateStr[-13],dateStr[-1])
            #每次月进行回测时,往前推进周期为12个月,基于前面24个月表现,决定后面一个月的持仓配置情况,所以月回测,循环12次
            for j in range(self.oosWindowM): 
                t0=time.time()
                print j
            #得到每月持仓时需要的24个月的时间戳
                btDateStr=dateStr[j:j+self.windowYear*self.oosWindowM]
            #计算每月(24个月的窗口)的因子系数,用24次计算的因子系数平均得出理论因子系数,因此每个月是跑24次循环,总共跑12个月
            #基于wind接口的betaMean
                [betaMean,standadizedFactorsList]=Barra().multiRegressWind(btDateStr,matlab,self.indexCode)
            #打分选股，市场中性构成组合，比较业绩
                selectedStockCode[btDateStr[-1]]=Barra().scoreWind(btDateStr,betaMean,self.scorePercent,standadizedFactorsList,self.indexCode)
                selectedStockWeight[btDateStr[-1]]=Barra().getIndustryNeutralWeight(btDateStr[-1],self.benchmark,selectedStockCode[btDateStr[-1]])  
                portReturn+=dataOperation().portReturn(selectedStockWeight[btDateStr[-1]],btDateStr[-1],dateStr[j+self.windowYear*self.oosWindowM])[1:]

            #逐年画图
            cumPR=list(np.cumsum(portReturn))
            cumBR=list(np.cumsum(benchmarkReturn))
            absoluteR=[(x-y)*1.00 for x,y in zip(cumPR,cumBR)]
       
            dataOperation().drawBacktest(cumBR,cumPR,dailyDate,self.path,backtestYear[i])
            dataOperation().drawAbsRBacktest(absoluteR,dailyDate,self.path,backtestYear[i])
            
backtestYear=['2012','2013']

if __name__ == "__main__": 
    #startDate='2007-01-01'
    #endDate='2013-12-31'
    #multiFactors().backtestPrepare(startDate,endDate)
    multiFactors().backtestCal(backtestYear)
    print 'finish'