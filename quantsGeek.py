    #-*- coding=utf-8 -*-
import ConfigParser
import xlwt
import os
import WindPy as wind
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.pylab as pylab
import matplotlib.dates as matDate
import math
import rpy2.robjects as robjects
import pymongo
from pymongo import ASCENDING, DESCENDING
import time
import datetime
import xlrd as xl
from xlutils.copy import copy
import pymat2 as pymat
import pymssql
import socket
import talib
import datetime
import pickle

#check the copywrite 
currentDate=datetime.datetime.now()
expireDate=datetime.datetime.strptime("2014-12-30",'%Y-%m-%d')
if currentDate>expireDate:
    raise Exception("You don't have the copywrite of QuantsGeek, please contact the provider")
    

class dataOperation(): 
    def __init__(self):
        self.ip=socket.gethostbyname(socket.gethostname())
        
    def getFiscalDateWind(self,date):
            result = {"0" : '09-30',"1" : '12-31',"2" : '06-30',"3" : '09-30'}
            base=[[1,2,3,4],[5,6,7,8],[9,10],[11,12]]
            res=str([int(date[5:7]) in x for x in base].index(True))
            if res=='0' or res== '1':
                    dateNew=str(int(date[:4])-1)+'-'+result.get(res)
            else:dateNew=date[:5]+result.get(res)
            return dateNew    
        
    def rawMultiFactorsRetrieveWind(self,date,multiFactors,factorsStr,indexCode,wind):
        print date
        multiFactors[date]=self.multiFactorsRetrieveWind(date, indexCode, factorsStr,wind)
        
    def r2p(self,a):
        row=robjects.r.dim(a)[0]
        col=robjects.r.dim(a)[1]
        return np.array([x for x in a]).reshape(col,row).transpose()
       
    def p2r(self,a):
        row=a.shape[0]
        col=a.shape[1]
        return robjects.r.matrix(list(a.transpose().reshape(1,row*col)[0]),ncol=col)
    
    def trans(self,x):
        x[0]=self.getdate(x[0])    
    
    def insertExcelData(self,path,database,collection):
        wb=xl.open_workbook(path)
        names=wb.sheet_by_index(0).row_values(0,1)
        names=["_id"]+names[1:]
        nrows=wb.sheet_by_index(0).nrows
        data=[]
        for i in range(nrows-2):
            data.append(wb.sheet_by_index(0).row_values(i+2,1))
            
        [self.trans(x) for x in data]    
        finalDict=[dict(zip(names,x)) for x in data]
        connection=pymongo.Connection("localhost",27017)
        exec("db=connection.%s"%(database,user,password))
        exec("db.%s.drop()"%(collection))
        exec("db.%s.insert(finalDict)"%(collection))
        print "finish"
   
    def strToDate(self,str):
        return datetime.datetime(int(str[0:4]),int(str[5:7]),int(str[8:]))
    
    def dateToFloat(self,date):
        baseDate=datetime.datetime(1900,01,01)  
        return (date-baseDate).days    
    
    def matNumToDateStr(self,num):
        baseDate=datetime.datetime(0001,01,01)
        pythonDate=(baseDate + datetime.timedelta(days = num-367)) 
        return self.dateToStr(pythonDate)
        
    def getdate(self,date):
        __s_date = datetime.date(1899, 12, 31).toordinal()-1
        if isinstance(date, float):
            date = int(date)
            a = datetime.datetime.fromordinal(__s_date + date)
        return a
    
    def lackData(self,x):
        for i in range(len(x)):
            if math.isnan(x[i]):
                x[i]=0.00001
        return x         
         
    def priceToReturn(self,a):
        return [x*1.0/y for x,y in zip(np.diff(a),a[0:-1])]
    
    def getDateStr(self,startDate,endDate,frequency,database,user,password):           
        conn=pymssql.connect(host=self.ip,user=user,password=password,database=database)
        cur=conn.cursor()
        if frequency=='M':sql='select Date from QT_TradingDay where DATE between \'%s\' and \'%s\' and IfMonthEnd=\'1\' and IfTradingDay=\'1\' and SecuMarket=\'83\'' %(startDate,endDate)
        elif frequency=='W':sql='select Date from QT_TradingDay where DATE between \'%s\' and \'%s\' and IfWeekEnd=\'1\' and IfTradingDay=\'1\' and SecuMarket=\'83\'' %(startDate,endDate)
        elif frequency=='D':sql='select Date from QT_TradingDay where DATE between \'%s\' and \'%s\' and IfTradingDay=\'1\' and SecuMarket=\'83\'' %(startDate,endDate)
        cur.execute(sql)
        res=cur.fetchall() 
        date=[x[0].isoformat()[:10] for x in res]   
        cur.close()   
        conn.close()
        return date   
    
    def getDateStrWind(self,startDate,endDate,frequency,wind):      
        dateStr=[self.dateToStr(x) for x in wind.w.tdays(startDate,endDate,"Period=%s"%(frequency)).Data[0]]
        return dateStr
    
    def getDateStrDHM(self,startDate,endDate,matlab):             
        #matlab.eval("dateStr=datenum(DH_D_TR_MarketTradingday(1,'%s','%s'))"%(startDate,endDate))
        matlab.eval("res=DH_D_TR_IntervalDay('%s','%s',3,2)"%(startDate,endDate))
        matlab.eval("dateStr=datenum(res(:,2))")
        res=matlab.getArray("dateStr")
        dateStr=[self.matNumToDateStr(float(x)) for x in res]
        return dateStr    
      
    
    def getIndexComponent(self,indexCode,date,database,user,password):   
        conn=pymssql.connect(host=self.ip,user=user,password=password,database=database)
        cur=conn.cursor()  
        sql='select b.SecuCode,b.SecuAbbr from (select * from LC_IndexComponent where InDate < \'%s\' and (OutDate is Null or OutDate > \'%s\') and IndexInnerCode=(select InnerCode from SecuMain where SecuCode=\'%s\' and SecuCategory=\'4\')) as a left join (select * from SecuMain)as b on a.SecuInnerCode=b.InnerCode' %(date,date,indexCode)
        cur.execute(sql)
        print sql
        res=cur.fetchall() 
        codeList=[str(x[0]) for x in res]  
        cur.close()   
        conn.close()
        return codeList  
    
    def factorsRecify(self,factorsList,i):

        return [x[i] for x in factorsList]
    
    def getFactorsMatrix(self,factorsList,factorsSequence):
        finalList=[self.factorsRecify(factorsList,i) for i in range(len(factorsSequence))]
        R=finalList[0]
        factors=finalList[1:]
        return R,factors
    
    def standarizing(self,factorsList):
        tempList=[x for x in factorsList if x!=0]
        mean=np.mean(tempList)
        sigma=np.std(tempList)
        upLimit=mean+2*sigma
        lowLimit=mean-2*sigma
        
        for i in range(len(factorsList)):
            if factorsList[i]!=0 and factorsList[i]>upLimit:
                factorsList[i]=(upLimit-mean)/sigma
            elif factorsList[i]!=0 and factorsList[i]<lowLimit:
                factorsList[i]=(lowLimit-mean)/sigma
            elif factorsList[i]!=0 and factorsList[i]>lowLimit and factorsList[i]<upLimit:
                factorsList[i]=(factorsList[i]-mean)/sigma
        #factorsList=[None if x==0 else x for x in factorsList]
        return factorsList   

    def standarizingWind(self,factorsList):
        tempList=[x for x in factorsList if math.isnan(x)!=True]
        mean=np.mean(tempList)
        sigma=np.std(tempList)
        upLimit=mean+2*sigma
        lowLimit=mean-2*sigma
        
        for i in range(len(factorsList)):
            if math.isnan(factorsList[i])!=True and factorsList[i]>upLimit:
                factorsList[i]=(upLimit-mean)/sigma
            elif math.isnan(factorsList[i])!=True and factorsList[i]<lowLimit:
                factorsList[i]=(lowLimit-mean)/sigma
            elif math.isnan(factorsList[i])!=True and factorsList[i]>lowLimit and factorsList[i]<upLimit:
                factorsList[i]=(factorsList[i]-mean)/sigma
        factorsList=[mean if math.isnan(x)==True else x for x in factorsList]
        return factorsList  
    
    def standarizeDataWind(self,factors):
        multiFactorsList=[self.standarizingWind(x) for x in factors]
        return multiFactorsList  
    
    def standarizeData(self,factors):
        return [self.standarizing(x) for x in factors]
    
    def sigmaMeanStandarizing(self,factorsList):
        tempList=[x for x in factorsList if x!=0]
        mean=np.mean(tempList)
        sigma=np.std(tempList)
        return mean,sigma 
    
    def sigmaMeanStandarizeData(self,factors):
        return [list(self.sigmaMeanStandarizing(x)) for x in factors]    

    def getFiscalDate(self,date):
        result = {"0" : '12-31',"1" : '03-31',"2" : '06-30',"3" : '09-30',"4":'12-31'}
        base=[[12],[3,4,5],[6,7,8],[9,10,11],[1,2]]
        res=str([int(date[5:7]) in x for x in base].index(True))
        if res=='4':
            dateNew=str(int(date[:4])-1)+'-'+result.get(res)
        else:dateNew=date[:5]+result.get(res)
        return dateNew
        
    def fiscalFactor(self,code,date,database,user,password):
        date=self.getFiscalDate(date)
        conn=pymssql.connect(host=self.ip,user=user,password=password,database=database)
        cur=conn.cursor()  
        sql='select d.EPSTTM,d.OperatingRevenuePSTTM,d.RetainedEarningsPS,d.OperCashFlowPSTTM,d.ROETTM,d.NetProfitRatioTTM,d.TotalAssetGrowRate,d.CashEquivalentPS,d.EquityMultipler_DuPont from (select * from SecuMain where SecuCategory=\'1\' and SecuCode=\'%s\') as a  left join (select * from LC_MainIndexNew where EndDate=\'%s\') as d on a.CompanyCode=d.CompanyCode' %(code,date)
        cur.execute(sql)
        res=cur.fetchall() 
        fiscalFactorValue=[float(x) if x!=None else x for x in list([x for x in res][0])]
        cur.close()   
        conn.close()
        return fiscalFactorValue   
    
    def dateToStr(self,date):
        return date.strftime("%Y-%m-%d")
    
    def getMonthlyReturn(self,code,date,database,user,password):
        startDate=date[:8]+'01'
        endDate=datetime.datetime.strptime(date,"%Y-%m-%d").date() +datetime.timedelta(days=1)
        endDate=endDate.strftime("%Y-%m-%d")
        
        #get the stock price at the startdate        
        conn=pymssql.connect(host=self.ip,user=user,password=password,database=database)
        cur=conn.cursor()  
        sql='SELECT TOP 1 E.SecuCode,A.TradingDay,A.ClosePrice*ISNULL( B.AdjustingFactor,1) AS AdjustedClosePrice FROM QT_DailyQuote A LEFT JOIN QT_AdjustingFactor B ON A.InnerCode = B.InnerCode AND B.ExDiviDate = (SELECT TOP 1 ExDiviDate FROM QT_AdjustingFactor AS C WHERE  C.INNERCODE = B.InnerCode AND C.ExDiviDate <= A.TradingDay  ORDER BY C.ExDiviDate DESC) JOIN SecuMain E ON A.InnerCode=E.InnerCode WHERE E.SecuCategory=\'1\' AND E.SecuCode=\'%s\'  AND A.TradingDay <\'%s\'  ORDER BY TradingDay DESC'  %(code,startDate) 
        cur.execute(sql)
        res=cur.fetchone() 
        startStockPrice=res[2] if res!=None else None
        
        #get the stock price at the enddate
        sql='SELECT TOP 1 E.SecuCode,A.TradingDay,A.ClosePrice*ISNULL( B.AdjustingFactor,1) AS AdjustedClosePrice FROM QT_DailyQuote A LEFT JOIN QT_AdjustingFactor B ON A.InnerCode = B.InnerCode AND B.ExDiviDate = (SELECT TOP 1 ExDiviDate FROM QT_AdjustingFactor AS C WHERE  C.INNERCODE = B.InnerCode AND C.ExDiviDate <= A.TradingDay  ORDER BY C.ExDiviDate DESC) JOIN SecuMain E ON A.InnerCode=E.InnerCode WHERE E.SecuCategory=\'1\' AND E.SecuCode=\'%s\'  AND A.TradingDay <\'%s\'  ORDER BY TradingDay DESC'  %(code,endDate) 
        cur.execute(sql)
        res=cur.fetchone() 
        endStockPrice=endStockPrice=res[2] if res!=None else None      
        
        stockMonthlyReturn=endStockPrice/startStockPrice-1 if (startStockPrice !=None and endStockPrice !=None) else 0

        cur.close()  
        conn.close()
        return [stockMonthlyReturn] 
    
    def findLastTradingDay(self,date,riskWindow,database,user,password):
        eD=self.strToDate(date)
        lastCalendarDate=eD-datetime.timedelta(riskWindow*365)
        lastCalendarDate=lastCalendarDate.strftime("%Y-%m-%d")        
        conn=pymssql.connect(host=self.ip,user=user,password=password,database=database)
        cur=conn.cursor()  
        sql='select max(TradingDate) from QT_TradingDayNew where TradingDate<\'%s\' ' %(lastCalendarDate)
        cur.execute(sql)
        res=cur.fetchall()[0][0] 
        lastTradingDay=res.strftime("%Y-%m-%d")  
        cur.close()   
        conn.close()
        return lastTradingDay   
    
    def riskFactor(self,code,date,riskWindow,benchmark,database,user,password,matlab):
        endDate=date
        startDate=self.findLastTradingDay(endDate,riskWindow,database,user,password)
        #get stockR series
        conn=pymssql.connect(host=self.ip,user=user,password=password,database=database)
        cur=conn.cursor()  
        sql='select 1.0000*(ClosePrice-PrevClosePrice)/PrevClosePrice as dailyR from QT_DailyQuote where TradingDay between \'%s\' and \'%s\' and InnerCode=(select InnerCode from SecuMain where SecuCategory=\'1\' and SecuCode=\'%s\') order by TradingDay' %(startDate,endDate,code)
        cur.execute(sql)
        res=cur.fetchall() 
        stockR=[float(x) if x!=None else 0 for x in list([x[0] for x in res])]
        
        #get benchmarkR series
        sql='select 1.0000*(ClosePrice-PrevClosePrice)/PrevClosePrice as dailyR from QT_DailyQuote where TradingDay between \'%s\' and \'%s\' and InnerCode=(select InnerCode from SecuMain where  SecuCode=\'%s\' and SecuCategory=\'4\') order by TradingDay' %(startDate,endDate,benchmark)
        cur.execute(sql)
        res=cur.fetchall() 
        benchmarkR=[float(x) if x!=None else 0 for x in list([x[0] for x in res])]        
        cur.close()   
        conn.close()
        
        diff=len(benchmarkR)-len(stockR)
        benchmarkR=benchmarkR[diff:]
        
        stockSigma=np.std(stockR)*np.sqrt(252)
        stockCumR=np.average(stockR)*252
        
        stockSR=stockCumR/stockSigma 
        stockSR=0 if (stockCumR==0 and stockSigma==0) else stockSR
        matlab.putArray('stockR',stockR)
        matlab.putArray('benchmarkR',benchmarkR)
        matlab.eval("res=regress(stockR,[ones(size(benchmarkR)) benchmarkR])")
    
        [alpha,beta]=[matlab.getArray('res')[0][0],matlab.getArray('res')[1][0]]        
     
        return [alpha,beta,stockSigma ,stockSR]
    
    
    def valueFactor(self,code,date,database,user,password):
        date=self.getFiscalDate(date)
        conn=pymssql.connect(host=self.ip,user=user,password=password,database=database)
        cur=conn.cursor()  
        sql='select b.PE,b.PB,b.PS from (select * from SecuMain where SecuCategory=\'1\' and SecuCode=\'%s\') as a  left join (select * from LC_IndicesForValuation where EndDate=\'%s\') as b on a.InnerCode=b.InnerCode' %(code,date)
        cur.execute(sql)
        res=cur.fetchall() 
        valueFactorValue=[float(x) if x!=None else x for x in list([x for x in res][0])]
        cur.close()   
        conn.close()
        return valueFactorValue    
    
    def MVFactor(self,code,date,database,user,password): 
        conn=pymssql.connect(host=self.ip,user=user,password=password,database=database)
        cur=conn.cursor()  
        sql='select b.AFloats*c.ClosePrice as AFloatsMV,b.TotalShares *c.ClosePrice as TotalSharesMV from (select * from SecuMain where SecuCategory=\'1\' and SecuCode=\'%s\') as a  left join (select * from LC_ShareStru where EndDate=(select max(b.EndDate) from (select * from SecuMain where SecuCategory=\'1\' and SecuCode=\'%s\') as a left join (select * from LC_ShareStru where EndDate<\'%s\') as b on a.CompanyCode=b.CompanyCode )) as b on a.CompanyCode=b.CompanyCode left join (select * from QT_DailyQuote where TradingDay=\'%s\') as c on a.InnerCode=c.InnerCode' %(code,code,date,date)
        cur.execute(sql)
        res=cur.fetchall() 
        MVFactorValue=[float(x) if x!=None else x for x in list([x for x in res][0])]
        cur.close()   
        conn.close()
        return MVFactorValue  
    
    def getIndexIndustryWeight(self,indexCode,date,wind): 
        #得到指数成份
        res=wind.w.wset('IndexConstituent','date=%s;windcode=%s.SH'%(date,indexCode)).Data
        codeList=res[1]
        codeList=[str(x) for x in codeList]
        stocksWeight=res[-1]
        #将个股分类
        res1=wind.w.wsd("%s"%(",".join(codeList)),"industry_gicscode","ED0D","%s"%(date),"industryType=2;Fill=Previous").Data[0]
        industryList=[str(x) for x in res1]
        temp=list(set(industryList))
        absIndustryCode=temp
        #将个股,行业属性,权重生成合并List
        industryTuple=zip(industryList,codeList,stocksWeight)
        industrySum=[self.industrySum(x,industryTuple) for x in absIndustryCode]
        return industrySum,absIndustryCode
    
    def industrySum(self,industryCode,industryTuple):
        return sum([x[2] for x in industryTuple if x[0]==industryCode])
    
    def stockSum(self,industryCode,selectedTuple):
        return [x[0] for x in selectedTuple if x[1]==industryCode]
    
    def stockIndustryWeight(self,industryCode,selectedTuple):
        return [[x[0],x[1]] for x in selectedTuple if x[2]==industryCode]     
    
    def RR_PriceFactor(self,code,date,database,user,password):
        conn=pymssql.connect(host=self.ip,user=user,password=password,database=database)
        cur=conn.cursor()  
        sql='select b.CompositeRatingScore,b.TargetPriceAvg from (select * from SecuMain where SecuCategory=\'1\' and SecuCode=\'%s\') as a left join (select * from RR_RatingTargetPriceStatHis where WritingDate=(select max(b.WritingDate) from (select * from SecuMain where SecuCategory=\'1\' and SecuCode=\'%s\') as a left join (select * from RR_RatingTargetPriceStatHis where WritingDate<\'%s\') as b on a.InnerCode=b.InnerCode))as b on a.InnerCode=b.InnerCode' %(code,code,date)
        cur.execute(sql)
        res=cur.fetchall() 
        RR_PriceFactorValue=[float(x) if x!=None else x for x in list([x for x in res][0])]
        cur.close()   
        conn.close()
        return RR_PriceFactorValue   

    def RR_ProfitFactor(self,code,date,database,user,password):    
        conn=pymssql.connect(host=self.ip,user=user,password=password,database=database)
        cur=conn.cursor()  
        sql='select b.TEPSAvg,b.TNPAvg,b.TFPE,b.FNPDivarication from (select * from SecuMain where SecuCategory=\'1\' and SecuCode=\'%s\') as a left join (select * from RR_ProfitsForecastStatHis where EndDate=(select max(b.EndDate) from (select * from SecuMain where SecuCategory=\'1\' and SecuCode=\'%s\') as a left join (select * from RR_ProfitsForecastStatHis where EndDate<\'%s\') as b on a.InnerCode=b.InnerCode))as b on a.InnerCode=b.InnerCode' %(code,code,date)
        cur.execute(sql)
        res=cur.fetchall() 
        RR_ProfitFactorValue=[float(x) if x!=None else x for x in list([x for x in res][0])]
        cur.close()   
        conn.close()
        return RR_ProfitFactorValue     
    
    def multiFactorsRetrieve(self,date,indexCode,benchmark,database,user,password,selectedFactors):    
        conn=pymssql.connect(host=self.ip,user=user,password=password,database=database)
        cur=conn.cursor()  
        sql='select %s from monthlyData where date=\'%s\' and indexCode=\'%s\' and benchmark=\'%s\' ' %(selectedFactors,date,indexCode,benchmark)
        cur.execute(sql)
        res=cur.fetchall() 
        factorsList=[x for x  in res]
        factorsSequence=selectedFactors.split(",")
        [R,factors]=self.getFactorsMatrix(factorsList,factorsSequence)
        standadizedFactors=self.standarizeData(factors)       
        cur.close()   
        conn.close()
        return R,standadizedFactors
    
    def multiFactorsRetrieveWind(self,date,indexCode,factorsStr,wind):  
        [R,factors]=self.rawMultiWind(date,indexCode,wind,factorsStr)       
        standadizedFactors=self.standarizeDataWind(factors)       
        return R,standadizedFactors      
    
    def meanSigmaRetrieveWind(self,date,indexCode):    
        [R,factors]=self.rawMultiWind(date,indexCode)        
        standadizedFactors=self.standarizeDataWind(factors)       
        return R,standadizedFactors      
    
    def sigmaMeanRetrieve(self,date,indexCode,benchmark,database,user,password,selectedFactors):    
        conn=pymssql.connect(host=self.ip,user=user,password=password,database=database)
        cur=conn.cursor()  
        sql='select %s from monthlyData where date=\'%s\' and indexCode=\'%s\' and benchmark=\'%s\' ' %(selectedFactors,date,indexCode,benchmark)
        cur.execute(sql)
        res=cur.fetchall() 
        factorsList=[x for x  in res]
        factorsSequence=selectedFactors.split(",")
        [R,factors]=self.getFactorsMatrix(factorsList,factorsSequence)
        meanSigma=self.sigmaMeanStandarizeData(factors)       
        cur.close()   
        conn.close()
        return meanSigma      
    
    def sigmaMeanRetrieveWind(self,date,indexCode,benchmark,database,user,password,selectedFactors):    
        conn=pymssql.connect(host=self.ip,user=user,password=password,database=database)
        cur=conn.cursor()  
        sql='select %s from monthlyData where date=\'%s\' and indexCode=\'%s\' and benchmark=\'%s\' ' %(selectedFactors,date,indexCode,benchmark)
        cur.execute(sql)
        res=cur.fetchall() 
        factorsList=[x for x  in res]
        factorsSequence=selectedFactors.split(",")
        [R,factors]=self.getFactorsMatrix(factorsList,factorsSequence)
        meanSigma=self.sigmaMeanStandarizeData(factors)       
        cur.close()   
        conn.close()
        return meanSigma      
    
    def stockFactorsRetrieve(self,betaMean,code,indexCode,benchmark,database,user,password,selectedFactors,date):  
        indexCode=str(int(indexCode))
        benchmark=str(int(benchmark))
        code=str(int(code))        
        sigmaMeanList=self.sigmaMeanRetrieve(date, indexCode, benchmark, database, user,password, selectedFactors)
        conn=pymssql.connect(host=self.ip,user=user,password=password,database=database)
        cur=conn.cursor()  

        
        sql='select %s from monthlyData where date=\'%s\' and indexCode=\'%s\' and benchmark=\'%s\'  and code =\'%s\''%(selectedFactors,date,indexCode,benchmark,code)
        cur.execute(sql)
        res=cur.fetchall()   
        factorsList=[x for x in res[0][1:]]
        sFactorsList=[(x-y[0])/y[1] for x,y in zip(factorsList,sigmaMeanList)]
        
        sFactorsList=[1]+sFactorsList
        replicateR=np.sum([x*y for x,y in zip(betaMean,sFactorsList)])
        cur.close()   
        conn.close()
        return [replicateR,code]    
    
    def stockFactorsRetrieveWind(self,betaMean,date):       
        sigmaMeanList=self.sigmaMeanRetrieve(date, indexCode, benchmark, database, user,password, selectedFactors)
        conn=pymssql.connect(host=self.ip,user=user,password=password,database=database)
        cur=conn.cursor()  

        
        sql='select %s from monthlyData where date=\'%s\' and indexCode=\'%s\' and benchmark=\'%s\'  and code =\'%s\''%(selectedFactors,date,indexCode,benchmark,code)
        cur.execute(sql)
        res=cur.fetchall()   
        factorsList=[x for x in res[0][1:]]
        sFactorsList=[(x-y[0])/y[1] for x,y in zip(factorsList,sigmaMeanList)]
        
        sFactorsList=[1]+sFactorsList
        replicateR=np.sum([x*y for x,y in zip(betaMean,sFactorsList)])
        cur.close()   
        conn.close()
        return [replicateR,code]        
    
    def factorsInsert(self,factorsList,database,user,password):
        factorsList[0]=self.dateToFloat(self.strToDate(factorsList[0]))
        factorsList=[0.0 if x==None else x for x in factorsList]
        conn=pymssql.connect(host=self.ip,user=user,password=password,database=database)
        cur=conn.cursor()     
        sql = 'insert into monthlyData ([date],code,indexCode,benchmark,monthlyR,alpha,beta,sigma,SR,EPSTTM,OperatingRevenuePSTTM,RetainedEarningsPS,OperCashFlowPSTTM,ROETTM,NetProfitRatioTTM,TotalAssetGrowRate,CashEquivalentPS,EquityMultipler_DuPont ,PE,PB,PS,FloatMV,TotalAssetMV,CompositeRatingScore,TargetPriceAvg,TEPSAvg,TNPAvg,TFPE,FNPDivarication) values(%f,%s,%s,%s,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f)' \
        %(factorsList[0],factorsList[1],factorsList[2],factorsList[3],factorsList[4],factorsList[5],factorsList[6],factorsList[7],factorsList[8],factorsList[9],factorsList[10],factorsList[11],factorsList[12],factorsList[13],factorsList[14],factorsList[15],factorsList[16],factorsList[17],factorsList[18],factorsList[19],factorsList[20],factorsList[21],factorsList[22],factorsList[23],factorsList[24],factorsList[25],factorsList[26],factorsList[27],factorsList[28])
        cur.execute(sql)   
        conn.commit()
        cur.close()    
        conn.close()
        
    def windCodeGenerateY(self,factor,codeList,date):
        res=wind.w.wsd("%s"%(",".join(codeList)),"%s"%(factor),"ED0Y","%s"%(date),"Period=Y;Fill=Previous;PriceAdj=F").Data[0]  
        return [0 if x==None else (0 if math.isnan(x)==True else x)  for x in res]  

    def windCodeGenerateD(self,factor,codeList,date):
        res=wind.w.wsd("%s"%(",".join(codeList)),"%s"%(factor),"ED0D","%s"%(date),"Fill=Previous;PriceAdj=F;Days=Alldays;").Data[0]
        return [0 if x==None else (0 if math.isnan(x)==True else x)  for x in res]         
        
    def factorsOperations(self,btDateStr,frequency,database,localDatabase,user,password,indexCode,benchmark,windowYear,riskWindow,matlab):               
        for date in btDateStr:
            print date
            t=time.time()
            codeList=self.getIndexComponent(indexCode,date,database,user,password)
            factorsList=[]
            factor={}
            for code in codeList:
                factor[code]=[]
                factor[code]=self.getMonthlyReturn(code,date,database,user,password)+self.riskFactor(code,date,riskWindow,benchmark,database,user,password,matlab)+self.fiscalFactor(code,date,database,user,password)+self.valueFactor(code,date,database,user,password)+self.MVFactor(code,date,database,user,password)+self.RR_PriceFactor(code,date,database,user,password)+self.RR_ProfitFactor(code,date,database,user,password)
                factorsList=[date,code,indexCode,benchmark]+factor[code]
                self.factorsInsert(factorsList, localDatabase, user, password)
            print time.time()-t
        
    def getIndexComponentWind(self,date,indexCode):
        codeList=wind.w.wset('IndexConstituent','date=%s;windcode=%s.SH'%(date[0:4]+date[5:7]+date[8:],indexCode)).Data[1]
        codeList=[str(x) for x in codeList]
        return codeList   
    
    def getIndexComponentWindR(self,date,indexCode,wind):
        codeList=wind.w.wset('IndexConstituent','date=%s;windcode=%s.SH'%(date,indexCode)).Data[1]
        codeList=[str(x) for x in codeList]
        return codeList  
    
    def getIndustryMaxWeightCode(self,combo):
        return [x[0] for x in combo if x[1]==max([x[1] for x in combo])][0]
    
    def getIndexIndustryCandidate(self,date,indexCode,wind):
        res=wind.w.wset('IndexConstituent','date=%s;windcode=%s.SH'%(date,indexCode)).Data
        codeList=res[1]
        codeWeight=res[3]
        codeList=[str(x) for x in codeList]
        res1=wind.w.wsd("%s"%(",".join(codeList)),"industry_gicscode","ED0D","%s"%(date),"industryType=2;Fill=Previous").Data[0]
        stockByIndustry=[str(x) for x in res1]
        absIndustryCode=[str(x) for x in list(set(res1))]
        combo=zip(codeList,codeWeight,stockByIndustry)
        industryCombo=[self.stockIndustryWeight(x,combo) for x in absIndustryCode]
        maxWeightStock=[self.getIndustryMaxWeightCode(x) for x in industryCombo]
        industryCandidate=zip(absIndustryCode,maxWeightStock)
        
        return industryCandidate       
        
    def rawMultiWind(self,date,indexCode,wind,factorsStr):       
        codeList=wind.w.wset('IndexConstituent','date=%s;windcode=%s.SH'%(date,indexCode)).Data[1]
        codeList=[str(x) for x in codeList]
        factorsList=[]
        factor={}
        #Return
        res=wind.w.wsd("%s"%(",".join(codeList)),"pct_chg","ED0M","%s"%(date),"Period=M;Fill=Previous;PriceAdj=F")
        monthlyR=[x*0.01 for x in res.Data[0]]
        codeList=[str(x) for x in res.Codes]
        calDate=[date]*len(codeList)
        indexCodeList=[indexCode]*len(codeList)
        
        #riskFactors
        riskFactors=factorsStr[0]
        riskFactors=riskFactors.split(",")
        riskFactorsList=[self.windCodeGenerateD(x, codeList, date) for x in riskFactors]
        
        #fiscalFactors
        fiscalDate=self.getFiscalDateWind(date)
        fiscalFactors=factorsStr[1]
        fiscalFactors=fiscalFactors.split(",")
        fiscalFactorsList=[self.windCodeGenerateD(x, codeList, fiscalDate) for x in fiscalFactors]
        
        #valueFactors
        valueFactors=factorsStr[2]
        valueFactors=valueFactors.split(",")
        valueFactorsList=[self.windCodeGenerateD(x, codeList, fiscalDate) for x in valueFactors]  
        
        #MVFactors
        MVFactors=factorsStr[3]
        MVFactors=MVFactors.split(",")
        MVFactorsList=[self.windCodeGenerateD(x, codeList, date) for x in MVFactors]    
        
        multiFactorsTemp=[indexCodeList]+[codeList]+[calDate]+[monthlyR]+riskFactorsList+fiscalFactorsList+valueFactorsList+MVFactorsList 
        multiFactorsList=[self.factorsRecify(multiFactorsTemp,x) for x in range(len(multiFactorsTemp[0]))]
        
        return monthlyR,riskFactorsList+fiscalFactorsList+valueFactorsList+MVFactorsList 
    
    def stocksAssignWeight(self,selectedStocksCode,date):
        wind.w.start()
        res1=wind.w.wsd("%s"%(",".join(selectedStocksCode)),"close","ED0D","%s"%(date),"Fill=Previous;PriceAdj=F").Data[0]  
        price=[float(x) for x in res1]
        return [1.00*x/sum(price) for x in price]
    
    def add(self,x, y): return x+y 
    
    def portReturn(self,trade,startDate,endDate,wind):
        stockList=[x[0] for x in trade]
        stockWeight=np.array([x[1] for x in trade])
        stockReturn=np.array(wind.w.wsd("%s"%(",".join(stockList)),"pct_chg","%s"%(startDate),"%s"%(endDate),"Fill=Previous;PriceAdj=F").Data)
        portReturn=[x/10000.0 for x in list(stockReturn.transpose().dot(stockWeight))]
        return portReturn
    
    def getDailyReturn(self,code,startDate,endDate,wind):
        res=wind.w.wsd("%s.SH"%(code),"pct_chg","%s"%(startDate),"%s"%(endDate),"Fill=Previous;PriceAdj=F")
        date=res.Times
        stockReturn=[x/100.0 for x in res.Data[0]]
        return stockReturn[1:],date[1:]
    
    def getDailyReturnDM(self,code,startDate,endDate):
        wind.w.start()
        res=wind.w.wsd("%s.SH"%(code),"pct_chg","%s"%(startDate),"%s"%(endDate),"Fill=Previous;PriceAdj=F")
        date=res.Times
        stockReturn=[x/100.0 for x in res.Data[0]]
        return stockReturn[1:],date[1:]    
    
    def drawBacktest(self,indexReturn,benchmarkReturn,portReturn,portReturn1,dailyDate,path,name):
        pylab.clf()
        pylab.plot_date(pylab.date2num(dailyDate),portReturn,'r',linewidth=0.8,linestyle='-') 
        pylab.plot_date(pylab.date2num(dailyDate),benchmarkReturn,'g',linewidth=0.8,linestyle='-') 
        pylab.plot_date(pylab.date2num(dailyDate),portReturn1,'b',linewidth=0.8,linestyle='-') 
        pylab.plot_date(pylab.date2num(dailyDate),indexReturn,'y',linewidth=0.8,linestyle='-') 
        
        xtext = pylab.xlabel('Out-Of-Sample Date')  
        ytext = pylab.ylabel('Cumulative Return')  
        ttext = pylab.title('Portfolio Return Vs Benchmark Return') 
        pylab.grid(True)
        pylab.setp(ttext, size='large', color='r')   
        pylab.setp(xtext, size='large', weight='bold', color='g')  
        pylab.setp(ytext, size='large', weight='light', color='b')  
        
        yearFormat=matDate.DateFormatter('%Y%m')
        ax=pylab.gca()
        ax.xaxis.set_major_formatter(yearFormat)

        pylab.savefig('%s%sbacktest.png'%(path,name))
        
    def drawAbsRBacktest(self,portReturn,portReturn1,dailyDate,path,name):
        pylab.clf()
        pylab.plot_date(pylab.date2num(dailyDate),portReturn,'r',linewidth=0.8,linestyle='-') 
        pylab.plot_date(pylab.date2num(dailyDate),portReturn1,'b',linewidth=0.8,linestyle='-') 
        xtext = pylab.xlabel('Out-Of-Sample Date')  
        ytext = pylab.ylabel('Cumulative Return')  
        ttext = pylab.title('Portfolio Return Vs Benchmark Return') 
        pylab.grid(True)
        pylab.setp(ttext, size='large', color='r')   
        pylab.setp(xtext, size='large', weight='bold', color='g')  
        pylab.setp(ytext, size='large', weight='light', color='b')  
        
        yearFormat=matDate.DateFormatter('%Y%m%d')
        ax=pylab.gca()
        ax.xaxis.set_major_formatter(yearFormat)        
        
        pylab.savefig('%s%sAbsRbacktest.png'%(path,name))    
        
    def backtestAttribution(self,portReturn,matlab):
        cumReturn=252*(sum(portReturn)/len(portReturn))
        vol=np.std(portReturn)*np.sqrt(252)
        sharpeRatio=1.0000*cumReturn/vol
        matlab.eval("clear")
        matlab.putArray('portReturn',portReturn)
        matlab.eval("cumR=portReturn+1")
        matlab.eval("res=maxdrawdown(cumR,'arithmetic')")
        maxDrawDown=matlab.getArray('res')[0] 
        
        return cumReturn,vol,sharpeRatio,maxDrawDown
    
    def historicFactorsRetrieve(self,databaseName,databaseStatus,multiFactors,factorsStr,indexCode,wind,allDateStr):
        if databaseStatus=='True':
            [dataOperation().rawMultiFactorsRetrieveWind(x,multiFactors,factorsStr,indexCode,wind) for x in allDateStr]
            f=file('%s'%(databaseName),'w')
            pickle.dump((multiFactors),f)
            f.close
        else:
            f=file('%s'%(databaseName),'r')
            multiFactors=pickle.load(f)
            f.close        
        return multiFactors
     
                
class Barra(dataOperation): 
    
    def multiRegress(self,btDateStr,matlab,indexCode,benchmark,database,user,password,selectedFactors):
        betaMean=[]
        for date in btDateStr:
            [R,standadizedFactors]=self.multiFactorsRetrieve(date,indexCode,benchmark,database,user,password,selectedFactors)
            matlab.putArray("R",R)
            matlab.putArray("factors",standadizedFactors)
            matlab.eval("b =regress(R,[ones(size(R)) transpose(factors)])")
            betaList=matlab.getArray('b')
            betaList=[x[0] for x in betaList]
            betaMean.append(betaList)       
        betaMean=[self.factorsRecify(betaMean,i) for i in range(len(selectedFactors.split(',')[:]))]
        betaMean=[np.mean(x) for x in betaMean]
        return betaMean  
    
    def multiRegressWind(self,btDateStr,multiFactors,matlab,indexCode,wind):
        betaMean=[]
        beta1Mean=[]
        sigmaMeanList=[]
        standadizedFactorsList=[]
        for date in btDateStr:    
            [R,standadizedFactors]=multiFactors[date]
            matlab.eval("clear")
            matlab.putArray("R",R)
            matlab.putArray("factors",standadizedFactors)
            matlab.eval("b =regress(R,[ones(size(R)) transpose(factors)])")        
            betaList=matlab.getArray('b')
            betaList=[x[0] for x in betaList]
            betaMean.append(betaList)    
            
            matlab.eval("[bStep,se,pval,inmodel,stats,nextstep,history]=stepwisefit(transpose(factors),R,'penter',0.05,'premove',0.05)")
            matlab.eval("b0=stats.intercept")
            beta0=matlab.getArray("b0")[0]
            betaStepList=matlab.getArray("bStep")
            matlab.eval("inmodel=double(inmodel)")
            
            inmodel=matlab.getArray("inmodel")
            beta1List=[x[0] for x in betaStepList]
            beta1List=[beta0]+[x if y==1.0 else 0 for x,y in zip(beta1List,inmodel)]               
            beta1Mean.append(beta1List)
            
            standadizedFactorsList.append(standadizedFactors)
            
        betaMean=[self.factorsRecify(betaMean,i) for i in range(len(betaMean[0]))]
        betaMean=[np.mean(x) for x in betaMean]
        
        beta1Mean=[self.factorsRecify(beta1Mean,i) for i in range(len(beta1Mean[0]))]
        beta1Mean=[np.mean(x) for x in beta1Mean]     
        
        return betaMean,beta1Mean,standadizedFactorsList   
    
    def multiStepwiseWind(self,btDateStr,matlab,indexCode):
        betaMean1=[]
        for date in btDateStr:
            print date       
            [R,standadizedFactors]=self.multiFactorsRetrieveWind(date,indexCode)
            matlab.putArray("R",R)
            matlab.putArray("factors",standadizedFactors)
            matlab.eval("[b,se,pval,inmodel,stats,nextstep,history]=stepwisefit(transpose(factors),R)")
            matlab.eval("b0=stats.intercept")
            beta0=matlab.getArray("b0")[0]
            betaStepList=matlab.getArray("b")
            
            pval=matlab.getArray("pval")
            beta1List=[x[0] for x in betaStepList]
            pval=[x[0] for x in pval]
            beta1List=[beta0]+[x if y<0.05 else 0 for x,y in zip(beta1List,pval)]
            
            betaMean1.append(beta1List)    
            standadizedFactorsList.append(standadizedFactors)
            
        betaMean1=[self.factorsRecify(betaMean1,i) for i in range(len(betaMean1[0]))]
        betaMean1=[np.mean(x) for x in betaMean1]
        return betaMean,standadizedFactorsList      
    
       
    def score(self,btDateStr,betaMean,scorePercent,indexCode,benchmark,database, localDatabase,user, password,selectedFactors):
        stocksList=self.getIndexComponent(indexCode, btDateStr[-1], database, user, password)
        #对每个股票按综合Beta计算收益率,再排序打分,目前的规则是按最后一个交易的因子值与综合Beta相乘得到最后的得分
        res=[self.stockFactorsRetrieve(betaMean, code, indexCode, benchmark, localDatabase, user, password, selectedFactors,btDateStr[-1]) for code in stocksList]
        replicateR=[x[0] for x in res]
        code=[x[1] for x in res]
        stocksDict=dict(zip(code,replicateR))
        stocksRank=sorted(stocksDict.iteritems(), key=lambda d:d[1], reverse = True )
        stocksCodeList=[x[0] for x in stocksRank[0:int(scorePercent*len(stocksList))]]
        return stocksCodeList
    
    def scoreWind(self,btDateStr,betaMean,scorePercent,standadizedFactorsList,indexCode,wind):
        stocksList=self.getIndexComponentWindR(btDateStr[-1],indexCode,wind)
        #对每个股票按综合Beta计算收益率,再排序打分,目前的规则是按最后一个交易的因子值与综合Beta相乘得到最后的得分
        #标准化因子最后一个进来的时候是21个因子,每个因子500个股票
        stocksSFList=[[1.0]+self.factorsRecify(standadizedFactorsList[-1],i) for i in range(len(standadizedFactorsList[-1][0]))]
         #标准因子转换后变成500个list,每个list包括21个因子
        stockMatrix=np.array(stocksSFList)
        betaMeanMatrix=np.array(betaMean)
        replicateRMatrix=stockMatrix.dot(betaMeanMatrix)
        replicateR=list(replicateRMatrix)
        stocksDict=dict(zip(stocksList,replicateR))
        stocksRank=sorted(stocksDict.iteritems(), key=lambda d:d[1], reverse = True )
        stocksCodeList=[x[0] for x in stocksRank[0:int(scorePercent*len(stocksList))]]
        return stocksCodeList  
             
    def getIndustryNeutralWeight(self,date,benchmark,selectedStockCode,wind):
        #得到行业权重
        [industryWeight,industryCode]=self.getIndexIndustryWeight(benchmark,date,wind)
        #每个行业产生替代股票,目前选行业内最大的权重股票作为行业替代股票,万一后面行业内找不到股票的时候,为了行业中性,直接用这个股票替代
        industryCandidate=self.getIndexIndustryCandidate(date,benchmark,wind)
        #得到个股分类
        res1=wind.w.wsd("%s"%(",".join(selectedStockCode)),"industry_gicscode","ED0D","%s"%(date),"industryType=2;Fill=Previous").Data[0]
        selectedStockIndustry=[str(x) for x in res1]
        selected=zip(selectedStockCode,selectedStockIndustry)
        #按行业将个股代码进行分类
        stocksCodeByIndustry=[self.stockSum(x, selected) for x in industryCode]
        #修复行业内无股票的情况
        stocksCodeByIndustry=[[y[1]] if x==[] else x for x,y in zip(stocksCodeByIndustry,industryCandidate)]
        stockWeightWithinIndustry=[self.stocksAssignWeight(x,date) for x in stocksCodeByIndustry]
        #得到按行业的,每个个股的权重
        stockWeight=[list(np.array(x)*y) for x,y in zip(stockWeightWithinIndustry,industryWeight)]
        stockWeightList=reduce(self.add,stockWeight)
        #得到按行业的,每个个股的权重
        #将股票代码按行业顺序排序
        stockCodeList=reduce(self.add,stocksCodeByIndustry)
        #将股票代码和对应权重汇总
        return zip(stockCodeList,stockWeightList)
    
       
    def stepwiseRegress(self,R,factors,matlab):
        matlab.putArray("R",R)
        matlab.putArray("factors",factors)
        matlab.eval("[b,bint,r,rint,stats] =regress(R,[ones(size(R)) transpose(factors)])")
        betaList=matlab.getArray('b')[1:]
        betaList=[x[0] for x in betaList]
        return betaList      
    
    def portReturn(self,trade,startDate,endDate):
        wind.w.start()
        stockList=[x[0] for x in trade]
        stockWeight=np.array([x[1] for x in trade])
        stockReturn=np.array(wind.w.wsd("%s"%(",".join(stockList)),"pct_chg","%s"%(startDate),"%s"%(endDate),"Fill=Previous;PriceAdj=F").Data)
        portReturn=[x/10000.0 for x in list(stockReturn.transpose().dot(stockWeight))]
        return portReturn
        

        
    
