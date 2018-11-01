'''
Created on 19/10/2018

@author: jramsay
'''
import re
import yaml
import ast
import csv
import difflib
import sqlite3
import json
from lxml import etree
from LDSAPI import StaticFetch, Authentication
from six.moves.urllib.error import HTTPError

XST = 'https://data.linz.govt.nz/layer/{id}/metadata/iso/xml/'

INF = 'https://data.linz.govt.nz/services/api/v1/layers/{lid}/'
VER = 'https://data.linz.govt.nz/services/api/v1/layers/{lid}/versions/{ver}'
MET = 'https://data.linz.govt.nz/services/api/v1/layers/{lid}/versions/{ver}/metadata/iso/'

CAP = 'http://data.linz.govt.nz/services;key={key}/{svc}?service={svc}&version={ver}&request=GetCapabilities'
FTX = './wfs:FeatureTypeList/wfs:FeatureType'
TPTH = "./wfs:Title"
NPTH = "./wfs:Name"
HPTH = "./ows:Keywords/ows:Keyword"
WFSv = '2.0.0'
WMSv = '1.1.1'



with open('properties.yaml') as h: yprops = yaml.load(h)
#Set Namespace Bundle
NSX = yprops['namespaces']['ns2']
#CAPSFILTER = 'Hydrographic'
#METAFILTER = ('./gmd:contact/gmd:CI_ResponsibleParty/gmd:positionName/gco:CharacterString','National Hydrographer',0.85)
GRP_FILTER = 2006
    
class SQL3DB(object):
    
    RTBL = 'hydro'
    
    def __init__(self):
        self.rsql = sqlite3.connect(':memory:')
        self.rcur = self.rsql.cursor()
        self.init_db()

    def init_db(self):
        q = 'CREATE TABLE {} (id INTEGER)'.format(self.RTBL)
        self.rcur.execute(q)
        self.commit()
        
    def colchk(self,cols):
        '''Check requested column list against existing'''
        q = 'PRAGMA table_info({})'.format(self.RTBL)
        pres = [i[1] for i in self.rcur.execute(q).fetchall()]
        for cn in cols.split(','):
            if cn not in pres:
                self.coladd(cn)
        
    def coladd(self,field):
        '''Check for a named field in the table_info RS'''
        q = 'ALTER TABLE {} ADD COLUMN {} VARCHAR'.format(self.RTBL,field)
        print ('AC',q)
        self.rcur.execute(q)
        self.commit()
                
    def populate(self,lid,cn,cv):
        '''Populate the db'''
        self.colchk(cn)
        q = 'INSERT INTO {} (id,{}) VALUES ({},{})'.format(self.RTBL,cn,str(lid),cv)
        #print('IN',q)
        self.rcur.execute(q)
        self.commit()
        
    def output(self):
        h = self.rcur.execute('PRAGMA table_info({})'.format(self.RTBL)).fetchall()
        q = 'SELECT * from {}'.format(self.RTBL)
        self.rcur.execute(q)
        rows = self.rcur.fetchall()
        with open("hydro.csv", "w") as csvfile:
            writer = csv.writer(csvfile)
            head = [i[1] for i in h]
            writer.writerow(head)
            for r in rows:
                writer.writerow(r)

        
    def commit(self):
        self.rsql.commit()
        
    def close(self):
        self.commit()
        self.rsql.close()
    
class LDSRead(object):
    #{'kfile':'.apikeyHEx'}
    korb = {'kfile':'.apikeyHEx'}#{'key':KEY}
    parser = etree.XMLParser(ns_clean=True, recover=True, encoding='utf-8')
    
    def __init__(self):
        pass
    
    @classmethod
    def getInfo(cls,lid):
        '''Get layer info page''' 
        content = StaticFetch.get(INF.format(lid=lid),korb=cls.korb).read().decode()
        try:
            dic = json.loads(content)
        except:
            dic = ast.literal_eval(content)
        #return a subset of the info
        return {k: dic.get(k, None) for k in ('group', 'version', 'metadata')}
    
    @staticmethod
    def drill(dic,pth):
        '''Trace a defined dict path which may be incomplete'''
        #print (pth,'##',dic)
        if pth and isinstance(dic,dict) and pth[0] in dic: 
            return LDSRead.drill(dic[pth[0]],pth[1:])
        else: return not len(pth),dic
    
    @classmethod
    def getVerURL(cls,lid): 
        '''Get the latest metadata version number''' 
        v = cls.getInfo(lid)
        return v['url']
    
    def idlist(self,url):
        '''Simple id extract from getcaps'''
        retry = 5
        ret = {'layer':(),'table':()}
        while retry:
            try:
                content = StaticFetch.get(url,korb=self.korb)
                text = content.read()
                tree = etree.fromstring(text, parser=self.parser)
                for ft in tree.findall(FTX, namespaces=NSX):
                    #\d+ finds either v:x-NNN or layer-NNN but also table-NNN
                    if CAPSFILTER and not any([re.search(CAPSFILTER,t.text) for t in ft.findall(HPTH, namespaces=NSX)]):
                        continue
                    match = re.search('(layer|table)-(\d+)',ft.find(NPTH, namespaces=NSX).text)
                    lort = match.group(1)
                    name = int(match.group(2))
                    title = ft.find(TPTH, namespaces=NSX).text
                    ret[lort] += ((name,title),)
                #return layer not table...
                return ret
            except HTTPError as he:
                print('RETRY',retry,str(he)[:1000])
                retry -= 1
            except Exception as e:
                print(e)
        #retries expired so...
        return ret
    
    def getids(self):
        k = Authentication.apikey(self.korb['kfile'])
        cap1 = CAP.format(key=k,svc='wfs',ver=WFSv)
        cap2 = CAP.format(key=k,svc='wms',ver=WMSv)
        return self.idlist(cap1),self.idlist(cap2)
    
    @classmethod
    def readurl(cls,lid):
        #u = MET.format(id=lid)
        info = cls.getInfo(lid)
        yes_meta,ver_url = cls.drill(info, ('metadata','iso'))
        if not yes_meta:
            print('No Metadata associated with this layer',lid)
            return
        yes_group,grp_val = cls.drill(info,('group','id'))
        if not yes_group or grp_val != GRP_FILTER:
            print('Layer {} does not belong to group {}!={}'.format(lid,grp_val,GRP_FILTER))
            return
            
        content = StaticFetch.get(ver_url,korb=cls.korb).read().decode()
        if re.search('<!DOCTYPE html>',content):
            print('HTML returned for {}, probably a private layer {}'.format(lid,content[:100]))
            return
        if METAFILTER:
            tree = etree.fromstring(content, parser=cls.parser)
            node = tree.find(METAFILTER[0], namespaces=NSX)
            if node is None:
                print ('No path to filter',METAFILTER[0])
                return
            elif not node.text or difflib.SequenceMatcher(None,METAFILTER[1],node.text).ratio()<METAFILTER[2]:
                print('Can\'t match filter {}!={}'.format(METAFILTER[1],node.text))
                return
        return content
    
def readfile(filename):
    with open(filename) as h:
        out = h.read()
    return out

def transform(ident, hydroreader, fnxsl = 's1.xsl'):
    res = None
    hydro_txt = hydroreader(ident)
    if not hydro_txt: return
    xsl_txt = readfile(fnxsl)
    try:
        hydro = etree.XML(hydro_txt)
        style = etree.XSLT(etree.XML(xsl_txt))
        res = style(hydro)
    except etree.XMLSyntaxError as xe:
        print('XML FAIL',ident,xe)
    except Exception as e:
        print('FAIL',ident,e)
        raise
    return res
        
def parse(res):
    '''parse the result to a dict and extract colnames and their values'''
    try:
        dic = json.loads(res)
    except:
        dic = ast.literal_eval(res)
    #eval DQ escapes get deleted in processing so put SQL3 escapes in
    dic = {k:v.replace('"','""') for k,v in dic.items()}
    colnames = ','.join(dic.keys())
    colvals = ','.join(['"{}"'.format(str(i)) for i in dic.values()])
    return colnames,colvals

def main():
    global CAPSFILTER,METAFILTER
    if 'CAPSFILTER' not in globals(): CAPSFILTER = None
    if 'METAFILTER' not in globals(): METAFILTER = None
    sq = SQL3DB()
    lds = LDSRead()
    wfsx,wmsx = lds.getids()
    for lid,_ in wfsx['layer']+wmsx['layer']:
    #for lid in [51779,50789,51247,51402,51779,]:
        print(lid)
        #res = transform(ident=filename,hydroreader=readfile,fnxsl='s6.xsl')
        res = transform(ident=lid,hydroreader=lds.readurl,fnxsl='s6.xsl')
        if res: sq.populate(lid,*parse(str(res)))

    sq.output()
    sq.close()
    

if __name__ == '__main__':
    main()